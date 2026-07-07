#include "../include/BZIP_lineBuffer.h"

#include <cstring>
#include <string>
#include <iostream>
#include <inttypes.h>

#include <bzlib.h>

using namespace std;

BZIP_lineBuffer::BZIP_lineBuffer(BZFILE* inputBuffer)
: mp_bzip2File(inputBuffer),
  m_index(0)
{
    m_bufferSize = INPUT_BUFFER_SIZE;
    m_inputbuffer = new char[m_bufferSize];

    m_available = BZ2_bzRead(&m_errorCode, mp_bzip2File, m_inputbuffer, m_bufferSize);
}

BZIP_lineBuffer::~BZIP_lineBuffer()
{
    delete []m_inputbuffer;
}

void BZIP_lineBuffer::readData( void )
{
    if(m_errorCode == BZ_OK)
    {
        m_index = 0;
        m_available = BZ2_bzRead(
            &m_errorCode, mp_bzip2File, m_inputbuffer, m_bufferSize);
    }
}

int BZIP_lineBuffer::get_error( )
{
    return m_errorCode;
}

// return non zero if the operation could not be done
int BZIP_lineBuffer::ReadByte(char& val)
{
    while(true)
    {
        if(m_index < m_available)
        {
            val = m_inputbuffer[m_index++];
            return 0;
        }

        if(m_errorCode == BZ_STREAM_END)
            return -1;

        if(m_errorCode != BZ_OK)
        {
            cerr << "ERROR, unable to read BZIP_lineBuffer" << endl;
            return -1;
        }

        readData();
    }
}

// return non zero if the operation could not be done
int BZIP_lineBuffer::ReadLine( string& str )
{
    str.clear();
    while(true)
    {
        const int remaining = m_available - m_index;
        const void* newline = memchr(m_inputbuffer + m_index, '\n', remaining);
        if(newline != NULL)
        {
            const char* end = static_cast<const char*>(newline) + 1;
            const int length = end - (m_inputbuffer + m_index);
            str.append(m_inputbuffer + m_index, length);
            m_index += length;
            return 0;
        }

        if(remaining > 0)
        {
            str.append(m_inputbuffer + m_index, remaining);
            m_index = m_available;
        }

        if(m_errorCode == BZ_STREAM_END)
            return str.empty() ? -1 : 0;

        if(m_errorCode != BZ_OK)
        {
            cerr << "ERROR, unable to read BZIP_lineBuffer" << endl;
            return -1;
        }

        readData();
    }
}
