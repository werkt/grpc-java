package io.grpc.netty;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.grpc.Metadata;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;

class NettyHttpResponseObserver extends HttpResponseObserver {
  private final NettyHttp1ServerStream stream;

  private boolean closed = false;
  private byte[] buffer = new byte[1024];
  private int inIndex = 0;
  private int outIndex = 0;
  private boolean flipped = false;

  private class ResponseInputStream extends InputStream {
    @Override
    public int available() {
      return inAvailable();
    }

    @Override
    public int read() throws IOException {
      if (inAvailable() <= 0) {
        throw new IOException("read must not be blocking");
        /* return -1; */
      }
      int b = buffer[inIndex];
      if (++inIndex == buffer.length) {
        inIndex = 0;
        flipped = false;
      }
      return b;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
      int totalBytesRead = 0;
      boolean eos = true;
      while (len > 0 && inAvailable() > 0) {
        int bytesRead = readPartial(buf, off, len);
        if (bytesRead > 0) {
          off += bytesRead;
          len -= bytesRead;
          totalBytesRead += bytesRead;
        }
        eos = bytesRead < 0;
      }
      if (totalBytesRead == 0 && eos) {
        return -1;
      }
      return totalBytesRead;
    }
  }

  NettyHttpResponseObserver(NettyHttp1ServerStream stream) {
    this.stream = stream;
    stream.writeMessage(new ResponseInputStream());
  }

  private int inAvailableBeforeClose() {
    if (!flipped) {
      return outIndex - inIndex;
    }
    return buffer.length - inIndex + outIndex;
  }

  private int inAvailable() {
    int beforeClose = inAvailableBeforeClose();
    if (beforeClose > 0 || !closed) {
      return beforeClose;
    }
    return -1;
  }

  private void close(HttpResponseStatus status) {
    closed = true;
    stream.close(status, new Metadata());
  }

  private int readPartial(byte[] buf, int off, int len) {
    if (len <= 0) {
      return 0;
    }
    int available = inAvailable();
    if (available <= 0) {
      return available;
    }
    int bytesToRead = Math.min(available, len);
    if (flipped) {
      bytesToRead = Math.min(bytesToRead, buffer.length - inIndex);
    }
    System.arraycopy(buffer, inIndex, buf, off, bytesToRead);
    int indexAfterRead = inIndex + bytesToRead;
    if (indexAfterRead == buffer.length) {
      indexAfterRead = 0;
      flipped = false;
    }
    inIndex = indexAfterRead;
    return bytesToRead;
  }

  @Override
  public void onCompleted() {
    close(OK);
  }

  @Override
  public void onError(HttpResponseStatus status) {
    close(status);
  }

  @Override
  public void setHeader(String key, String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(int c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(byte[] buf, int off, int len) throws IOException {
    while (len > 0) {
      int bytesWritten = writePartial(buf, off, len);
      if (bytesWritten > 0) {
        off += bytesWritten;
        len -= bytesWritten;
      }
    }
    if (len != 0) {
      throw new IOException("could not complete write");
    }
  }

  private int outAvailable() {
    if (!flipped) {
      return buffer.length - outIndex;
    }
    return inIndex - outIndex;
  }

  private int writePartial(byte[] buf, int off, int len) throws IOException {
    int available = outAvailable();
    if (available <= 0) {
      stream.flush();
      available = outAvailable();
    }
    if (available <= 0) {
      return available;
    }
    if (!flipped) {
      available = Math.min(available, buffer.length - outIndex);
    }
    int bytesToWrite = Math.min(available, len);
    System.arraycopy(buf, off, buffer, outIndex, bytesToWrite);
    int indexAfterWrite = outIndex + bytesToWrite;
    if (indexAfterWrite == buffer.length) {
      indexAfterWrite = 0;
      flipped = true;
    }
    outIndex = indexAfterWrite;
    return bytesToWrite;
  }
}
