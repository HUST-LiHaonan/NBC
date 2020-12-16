/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: TextPair
 * Author:   mac
 * Date:     2020/10/2 10:45 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package WordCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/2
 * @since 1.0.0
 */
public class TextPair implements WritableComparable<TextPair> {
    private Text classname;
    private Text word;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String classname, String word) {
        set(new Text(classname), new Text(word));
    }

    public TextPair(Text classname, Text word) {
        set(classname, word);
    }

    public void set(Text dirName, Text word) {
        this.classname = dirName;
        this.word = word;
    }

    public Text getFirst() {
        return classname;
    }

    public Text getSecond() {
        return word;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        classname.write(out);
        word.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        classname.readFields(in);
        word.readFields(in);
    }

    @Override
    public String toString() {
        return classname + "\t" + word;
    }

    @Override
    public int compareTo(TextPair textPair) {
        int cmp = classname.compareTo(textPair.classname);
        if (cmp !=0) {
            return cmp;
        }
        return word.compareTo(textPair.word);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TextPair textPair = (TextPair) o;

        if (classname != null ? !classname.equals(textPair.classname) : textPair.classname != null) {
            return false;
        }
        return word != null ? word.equals(textPair.word) : textPair.word == null;
    }

    @Override
    public int hashCode() {
        int result = classname != null ? classname.hashCode() : 0;
        result = 31 * result + (word != null ? word.hashCode() : 0);
        return result;
    }
}
