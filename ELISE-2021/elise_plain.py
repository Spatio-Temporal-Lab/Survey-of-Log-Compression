#!/usr/bin/env python3
"""Run ELISE-style LSTM arithmetic coding on plain log files."""

import argparse
import json
import struct
import tempfile
from pathlib import Path

import numpy as np

from audit import arithmeticcoding_fast


MAGIC = b"ELISEP1\0"
HEADER = struct.Struct("<8sQIIQI")
LENGTH = struct.Struct("<Q")
ALPHABET_SIZE = 256
FREQUENCY_SCALE = 10_000_000


def sigmoid(value):
    value = np.clip(value, -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-value))


class NumpyLSTM:
    def __init__(self, timesteps, hidden_size=24, seed=42):
        self.timesteps = timesteps
        self.hidden_size = hidden_size
        rng = np.random.default_rng(seed)
        scale = 0.08
        self.weights = {
            "embedding": rng.normal(
                0, scale, (ALPHABET_SIZE, hidden_size)
            ).astype(np.float32),
            "input": rng.normal(
                0, scale, (hidden_size, 4 * hidden_size)
            ).astype(np.float32),
            "recurrent": rng.normal(
                0, scale, (hidden_size, 4 * hidden_size)
            ).astype(np.float32),
            "bias": np.zeros(4 * hidden_size, dtype=np.float32),
            "output": rng.normal(
                0, scale, (hidden_size, ALPHABET_SIZE)
            ).astype(np.float32),
            "output_bias": np.zeros(ALPHABET_SIZE, dtype=np.float32),
        }
        self.step = 0
        self.first_moment = {
            key: np.zeros_like(value) for key, value in self.weights.items()
        }
        self.second_moment = {
            key: np.zeros_like(value) for key, value in self.weights.items()
        }

    def forward(self, contexts, keep_cache=False):
        batch_size = contexts.shape[0]
        hidden = np.zeros(
            (batch_size, self.hidden_size), dtype=np.float32
        )
        cell = np.zeros_like(hidden)
        cache = []
        for position in range(self.timesteps):
            previous_hidden = hidden
            previous_cell = cell
            embedded = self.weights["embedding"][contexts[:, position]]
            gates = (
                embedded @ self.weights["input"]
                + previous_hidden @ self.weights["recurrent"]
                + self.weights["bias"]
            )
            i, f, g, o = np.split(gates, 4, axis=1)
            i, f, o = sigmoid(i), sigmoid(f), sigmoid(o)
            g = np.tanh(g)
            cell = f * previous_cell + i * g
            hidden = o * np.tanh(cell)
            if keep_cache:
                cache.append(
                    (
                        contexts[:, position],
                        embedded,
                        previous_hidden,
                        previous_cell,
                        i,
                        f,
                        g,
                        o,
                        cell,
                    )
                )
        logits = hidden @ self.weights["output"] + self.weights["output_bias"]
        logits -= logits.max(axis=1, keepdims=True)
        probabilities = np.exp(logits)
        probabilities /= probabilities.sum(axis=1, keepdims=True)
        return probabilities, hidden, cache

    def predict(self, contexts):
        return self.forward(contexts, keep_cache=False)[0]

    def train_batch(self, contexts, targets, learning_rate):
        probabilities, hidden, cache = self.forward(contexts, keep_cache=True)
        batch_size = len(targets)
        loss = -np.log(
            np.maximum(probabilities[np.arange(batch_size), targets], 1e-12)
        ).mean()
        accuracy = np.mean(np.argmax(probabilities, axis=1) == targets)

        output_gradient = probabilities.copy()
        output_gradient[np.arange(batch_size), targets] -= 1
        output_gradient /= batch_size
        gradients = {
            key: np.zeros_like(value) for key, value in self.weights.items()
        }
        gradients["output"] = hidden.T @ output_gradient
        gradients["output_bias"] = output_gradient.sum(axis=0)
        hidden_gradient = output_gradient @ self.weights["output"].T
        cell_gradient = np.zeros_like(hidden_gradient)

        for values in reversed(cache):
            (
                tokens,
                embedded,
                previous_hidden,
                previous_cell,
                i,
                f,
                g,
                o,
                cell,
            ) = values
            tanh_cell = np.tanh(cell)
            output_gate_gradient = hidden_gradient * tanh_cell
            combined_cell_gradient = (
                cell_gradient
                + hidden_gradient * o * (1.0 - tanh_cell * tanh_cell)
            )
            input_gate_gradient = combined_cell_gradient * g
            candidate_gradient = combined_cell_gradient * i
            forget_gate_gradient = combined_cell_gradient * previous_cell
            cell_gradient = combined_cell_gradient * f
            gate_gradient = np.concatenate(
                (
                    input_gate_gradient * i * (1.0 - i),
                    forget_gate_gradient * f * (1.0 - f),
                    candidate_gradient * (1.0 - g * g),
                    output_gate_gradient * o * (1.0 - o),
                ),
                axis=1,
            )
            gradients["input"] += embedded.T @ gate_gradient
            gradients["recurrent"] += previous_hidden.T @ gate_gradient
            gradients["bias"] += gate_gradient.sum(axis=0)
            embedding_gradient = gate_gradient @ self.weights["input"].T
            np.add.at(gradients["embedding"], tokens, embedding_gradient)
            hidden_gradient = gate_gradient @ self.weights["recurrent"].T

        self.step += 1
        for key, gradient in gradients.items():
            np.clip(gradient, -5.0, 5.0, out=gradient)
            self.first_moment[key] = (
                0.9 * self.first_moment[key] + 0.1 * gradient
            )
            self.second_moment[key] = (
                0.999 * self.second_moment[key] + 0.001 * gradient * gradient
            )
            corrected_first = self.first_moment[key] / (1.0 - 0.9**self.step)
            corrected_second = self.second_moment[key] / (
                1.0 - 0.999**self.step
            )
            self.weights[key] -= (
                learning_rate
                * corrected_first
                / (np.sqrt(corrected_second) + 1e-8)
            )
        return float(loss), float(accuracy)

    def save(self, path):
        with Path(path).open("wb") as output:
            np.savez(
                output,
                timesteps=np.array(self.timesteps),
                hidden_size=np.array(self.hidden_size),
                **self.weights,
            )

    @classmethod
    def load(cls, path):
        with np.load(path) as saved:
            model = cls(
                int(saved["timesteps"]),
                hidden_size=int(saved["hidden_size"]),
            )
            for key in model.weights:
                model.weights[key] = saved[key]
        return model


def uniform_cumulative():
    return np.arange(ALPHABET_SIZE + 1, dtype=np.uint64)


def probability_cumulative(probabilities):
    counts = (probabilities * FREQUENCY_SCALE).astype(np.uint64) + 1
    cumulative = np.empty(ALPHABET_SIZE + 1, dtype=np.uint64)
    cumulative[0] = 0
    cumulative[1:] = np.cumsum(counts, dtype=np.uint64)
    return cumulative


def load_bytes(path, max_bytes=None):
    data = Path(path).read_bytes()
    if max_bytes is not None:
        data = data[:max_bytes]
    return np.frombuffer(data, dtype=np.uint8)


def train(args):
    series = load_bytes(args.input, args.max_bytes)
    if len(series) <= args.timesteps:
        raise ValueError("input must be longer than --timesteps")
    model = NumpyLSTM(args.timesteps, args.hidden_size, args.seed)
    rng = np.random.default_rng(args.seed)
    positions = np.arange(len(series) - args.timesteps)
    offsets = np.arange(args.timesteps)

    for epoch in range(args.epochs):
        rng.shuffle(positions)
        losses, accuracies = [], []
        for start in range(0, len(positions), args.batch_size):
            selected = positions[start : start + args.batch_size]
            contexts = series[selected[:, None] + offsets]
            targets = series[selected + args.timesteps]
            loss, accuracy = model.train_batch(
                contexts, targets, args.learning_rate
            )
            losses.append(loss)
            accuracies.append(accuracy)
        print(
            f"epoch={epoch + 1} loss={np.mean(losses):.4f} "
            f"accuracy={np.mean(accuracies):.4f}"
        )
    model.save(args.model)
    Path(str(args.model) + ".json").write_text(
        json.dumps(
            {
                "format": "ELISE plain-log NumPy LSTM",
                "training_bytes": int(len(series)),
                "timesteps": args.timesteps,
                "hidden_size": args.hidden_size,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def encode_streams(model, sequences, directory):
    streams, stream_length = sequences.shape
    files, encoders = [], []
    for index in range(streams):
        path = Path(directory) / f"stream-{index:04d}.bin"
        raw = path.open("wb")
        bitout = arithmeticcoding_fast.BitOutputStream(raw)
        files.append((path, bitout))
        encoders.append(arithmeticcoding_fast.ArithmeticEncoder(32, bitout))
    uniform = uniform_cumulative()
    for stream in range(streams):
        for position in range(min(model.timesteps, stream_length)):
            encoders[stream].write(uniform, int(sequences[stream, position]))
    for position in range(model.timesteps, stream_length):
        contexts = sequences[:, position - model.timesteps : position]
        probabilities = model.predict(contexts)
        for stream in range(streams):
            encoders[stream].write(
                probability_cumulative(probabilities[stream]),
                int(sequences[stream, position]),
            )
    for encoder, (_, bitout) in zip(encoders, files):
        encoder.finish()
        bitout.close()
    return [item[0] for item in files]


def compress(args):
    series = load_bytes(args.input)
    model = NumpyLSTM.load(args.model)
    streams = min(args.streams, max(1, len(series)))
    main_length = len(series) // streams * streams
    stream_length = main_length // streams
    sequences = series[:main_length].reshape(streams, stream_length)
    remainder = series[main_length:].tobytes()
    with tempfile.TemporaryDirectory() as temp_dir:
        paths = encode_streams(model, sequences, temp_dir)
        with Path(args.output).open("wb") as output:
            output.write(
                HEADER.pack(
                    MAGIC,
                    len(series),
                    streams,
                    model.timesteps,
                    stream_length,
                    len(remainder),
                )
            )
            for path in paths:
                encoded = path.read_bytes()
                output.write(LENGTH.pack(len(encoded)))
                output.write(encoded)
            output.write(remainder)


def decode_streams(model, paths, length):
    result = np.zeros((len(paths), length), dtype=np.uint8)
    raw_files = [Path(path).open("rb") for path in paths]
    bit_inputs = [
        arithmeticcoding_fast.BitInputStream(raw) for raw in raw_files
    ]
    decoders = [
        arithmeticcoding_fast.ArithmeticDecoder(32, bitin)
        for bitin in bit_inputs
    ]
    uniform = uniform_cumulative()
    for stream, decoder in enumerate(decoders):
        for position in range(min(model.timesteps, length)):
            result[stream, position] = decoder.read(uniform, ALPHABET_SIZE)
    for position in range(model.timesteps, length):
        contexts = result[:, position - model.timesteps : position]
        probabilities = model.predict(contexts)
        for stream, decoder in enumerate(decoders):
            result[stream, position] = decoder.read(
                probability_cumulative(probabilities[stream]), ALPHABET_SIZE
            )
    for bitin in bit_inputs:
        bitin.close()
    return result


def decompress(args):
    model = NumpyLSTM.load(args.model)
    with Path(args.input).open("rb") as archive:
        magic, original_length, streams, timesteps, stream_length, tail_len = (
            HEADER.unpack(archive.read(HEADER.size))
        )
        if magic != MAGIC:
            raise ValueError("not an ELISE plain-log archive")
        if model.timesteps != timesteps:
            raise ValueError("archive and model use different timesteps")
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = []
            for index in range(streams):
                encoded_length = LENGTH.unpack(archive.read(LENGTH.size))[0]
                path = Path(temp_dir) / f"stream-{index:04d}.bin"
                path.write_bytes(archive.read(encoded_length))
                paths.append(path)
            remainder = archive.read(tail_len)
            decoded = decode_streams(model, paths, stream_length)
    restored = decoded.reshape(-1).tobytes() + remainder
    if len(restored) != original_length:
        raise ValueError("decompressed length does not match archive header")
    Path(args.output).write_bytes(restored)


def create_parser():
    parser = argparse.ArgumentParser(
        description="ELISE-style LSTM arithmetic coding for plain logs."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    train_parser = commands.add_parser("train")
    train_parser.add_argument("-i", "--input", required=True)
    train_parser.add_argument("-m", "--model", required=True)
    train_parser.add_argument("--max-bytes", type=int)
    train_parser.add_argument("--timesteps", type=int, default=24)
    train_parser.add_argument("--hidden-size", type=int, default=24)
    train_parser.add_argument("--batch-size", type=int, default=256)
    train_parser.add_argument("--epochs", type=int, default=1)
    train_parser.add_argument("--learning-rate", type=float, default=0.001)
    train_parser.add_argument("--seed", type=int, default=42)
    train_parser.set_defaults(func=train)
    compress_parser = commands.add_parser("compress")
    compress_parser.add_argument("-i", "--input", required=True)
    compress_parser.add_argument("-m", "--model", required=True)
    compress_parser.add_argument("-o", "--output", required=True)
    compress_parser.add_argument("--streams", type=int, default=64)
    compress_parser.set_defaults(func=compress)
    decompress_parser = commands.add_parser("decompress")
    decompress_parser.add_argument("-i", "--input", required=True)
    decompress_parser.add_argument("-m", "--model", required=True)
    decompress_parser.add_argument("-o", "--output", required=True)
    decompress_parser.set_defaults(func=decompress)
    return parser


if __name__ == "__main__":
    arguments = create_parser().parse_args()
    arguments.func(arguments)
