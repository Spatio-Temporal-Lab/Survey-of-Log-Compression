/*
 * glibc_compat.c
 *
 * On systems built with glibc >= 2.38 + GCC 13+, fscanf calls are compiled
 * as __isoc23_fscanf (C23 ABI).  Binaries that contain such calls cannot run
 * on systems with glibc < 2.38.
 *
 * This file provides __isoc23_fscanf as a thin wrapper around __isoc99_vfscanf
 * (available since glibc 2.7), so the compiled binaries are compatible with
 * any glibc >= 2.7.
 *
 * IMPORTANT: compile this file WITHOUT _GNU_SOURCE (use -D_POSIX_C_SOURCE=200809L)
 * so that vfscanf itself is not also redirected to the C23 variant.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdarg.h>

int __isoc23_fscanf(FILE *f, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int r = vfscanf(f, fmt, ap);
    va_end(ap);
    return r;
}