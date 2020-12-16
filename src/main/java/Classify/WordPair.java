/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: WordPair
 * Author:   mac
 * Date:     2020/10/6 9:07 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Classify;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/6
 * @since 1.0.0
 */
public class WordPair {
    private String classname;
    private String word;

    public WordPair() {
    }

    public WordPair(String classname, String word) {
        this.classname = classname;
        this.word = word;
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WordPair wordPair = (WordPair) o;

        if (classname != null ? !classname.equals(wordPair.classname) : wordPair.classname != null) {
            return false;
        }
        return word != null ? word.equals(wordPair.word) : wordPair.word == null;
    }

    @Override
    public int hashCode() {
        int result = classname != null ? classname.hashCode() : 0;
        result = 31 * result + (word != null ? word.hashCode() : 0);
        return result;
    }
}
