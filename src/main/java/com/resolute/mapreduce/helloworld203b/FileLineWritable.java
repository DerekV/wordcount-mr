package com.resolute.mapreduce.helloworld203b;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileLineWritable implements WritableComparable<FileLineWritable> {
    public String fileName;
    public long offset;

    @Override
    public int compareTo(FileLineWritable that) {
        int cmp = this.fileName.compareTo(that.fileName);
        if (cmp != 0) return cmp;
        return (int)Math.signum((double)(this.offset - that.offset));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileLineWritable that = (FileLineWritable) o;

        if (offset != that.offset) return false;
        return fileName != null ? fileName.equals(that.fileName) : that.fileName == null;
    }

    @Override
    public int hashCode() {
        int result = fileName != null ? fileName.hashCode() : 0;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
