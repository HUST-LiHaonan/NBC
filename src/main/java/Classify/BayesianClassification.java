/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: BayesianClassification
 * Author:   mac
 * Date:     2020/10/4 7:53 上午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Classify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/4
 * @since 1.0.0
 */
public class BayesianClassification {
    //记录每一种类型的文本在训练集中出现的先验概率
    private static final Map<String,Double> TEXT_PRO = new ConcurrentHashMap<>();
    //记录每一种类型的文本中某一个单词出现的条件概率
    private static final Map<WordPair,Double> TEXT_WORD_PRO = new ConcurrentHashMap<>();
    //记录每一种类型的文本中出现单词的总数
    private static final Map<String,Double> TEXT_WORD_SIZE = new ConcurrentHashMap<>();
    //记录整个训练集中的单词集合
    private static final Set<String> WORD_SET = new ConcurrentHashSet<>();
    //操作HDFS所需的相关配置
    private static final Configuration configuration = new Configuration();
    private static FileSystem fileSystem;
    //记录文本总数，便于计算文本出现的概率
    private static double textSum = 0;
    //静态初始化
    static {
        try {
            fileSystem = FileSystem.get(new URI("hdfs://localhost:8020/"),configuration);
            //计算文本总数，并计算某一文本类型在训练集中出现的先验概率
            textSumAndPro();
            //计算某一文本类型中单词总数
            textWordSize();
            //计算全体训练集单词集合
            wordSet();
            //计算单词在某一类型的文本下出现的概率
            textWordPro();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 计算每一类别的文本在训练集中出现的概率
     * @throws IOException
     */
    private static void textSumAndPro() throws IOException {
        FSDataInputStream textIn = fileSystem.open(new Path("/zs/trainOutput/part-r-00000"));
        BufferedReader textReader = new BufferedReader(new InputStreamReader(textIn));
        String line;
        while ((line=textReader.readLine())!=null){
            String[] data = line.split("\t");
            TEXT_PRO.put(data[0],Double.parseDouble(data[1]));
            textSum += Double.parseDouble(data[1]);
        }
        TEXT_PRO.forEach((k, v)->{
            TEXT_PRO.compute(k, (s, aDouble) -> aDouble/textSum);
        });
        textIn.close();
        textReader.close();
        System.out.println("训练集文本总数是"+textSum);
        System.out.println("文本的先验概率是"+ TEXT_PRO);
    }

    private static void classPro() throws IOException {
        FSDataInputStream textIn = fileSystem.open(new Path("/zs/trainOutput/part-r-00000"));
        BufferedReader textReader = new BufferedReader(new InputStreamReader(textIn));
        String line;
        while ((line=textReader.readLine())!=null){
            String[] data = line.split("\t");
            TEXT_PRO.put(data[0],Double.parseDouble(data[1])/ textSum);
        }
        textIn.close();
        textReader.close();
        System.out.println("分类先验概率是"+ TEXT_PRO);
    }

    /**
     * 计算每一种类别的文本中出现的单词总数
     * @throws IOException
     */
    private static void textWordSize() throws IOException {
        FSDataInputStream wordIn = fileSystem.open(new Path("/zs/trainWordOutput/part-r-00000"));
        BufferedReader wordReader = new BufferedReader(new InputStreamReader(wordIn));
        String line;
        while ((line=wordReader.readLine())!=null){
            String[] data = line.split("\t");
            String classification = data[0];
            double num = Double.parseDouble(data[2]);
            TEXT_WORD_SIZE.merge(classification, num, Double::sum);
        }
        wordIn.close();
        wordReader.close();
        System.out.println("每一个分类中单词总数是"+ TEXT_WORD_SIZE);
    }

    private static void wordSet() throws IOException {
        FSDataInputStream wordIn = fileSystem.open(new Path("/zs/trainWordOutput/part-r-00000"));
        BufferedReader wordReader = new BufferedReader(new InputStreamReader(wordIn));
        String line;
        while ((line=wordReader.readLine())!=null){
            String[] data = line.split("\t");
            String word = data[1];
            WORD_SET.add(word);
        }
        wordIn.close();
        wordReader.close();
        System.out.println("训练集中单词集合的大小是"+ WORD_SET.size());
    }

    private static void textWordPro() throws IOException {
        FSDataInputStream wordIn = fileSystem.open(new Path("/zs/trainWordOutput/part-r-00000"));
        BufferedReader wordReader = new BufferedReader(new InputStreamReader(wordIn));
        String line;
        while ((line=wordReader.readLine())!=null){
            String[] data = line.split("\t");
            String classification = data[0];
            String word = data[1];
            double num = Double.parseDouble(data[2]);
            WordPair wordPair = new WordPair(classification,word);
            TEXT_WORD_PRO.put(wordPair,(num+1)/(TEXT_WORD_SIZE.get(classification)+ WORD_SET.size()));
        }
        wordIn.close();
        wordReader.close();
        System.out.println("训练集中拥有概率的单词数是"+ TEXT_WORD_PRO.size());
    }

    /**
     * 计算测试集中的本文属于某一类型的概率
     * @param text
     * @param classify
     * @return
     */
    public static double calculateProbability(String text, String classify){
        // 计算一个文本属于某一类型的条件概率
        double pro = 0;
        //文件读取时每一行的一次单词之间用'\t'进行分割
        String[] words = text.split("\t");
        for(String word:words){
            WordPair pair = new WordPair(classify,word);
            //单个单词概率太低如果连续相乘会出现0，故取对数进行计算
            pro += Math.log(TEXT_WORD_PRO.getOrDefault(pair,1.0/(TEXT_WORD_SIZE.get(classify) + WORD_SET.size())));
        }
        pro += Math.abs(Math.log(TEXT_PRO.get(classify)));
        return pro;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Class.forName("Classify.BayesianClassification");
        System.out.println(BayesianClassification.calculateProbability("china","USA"));
        System.out.println(BayesianClassification.calculateProbability("china","UK"));
    }
}
