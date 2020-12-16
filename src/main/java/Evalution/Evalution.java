/**
 * Copyright (C), 2016-2020, 华中科技大学
 * FileName: Evalution
 * Author:   mac
 * Date:     2020/10/14 7:40 下午
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package Evalution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author mac
 * @create 2020/10/14
 * @since 1.0.0
 */
public class Evalution {
    //操作HDFS所需的相关配置
    private static final Configuration configuration = new Configuration();
    private static FileSystem fileSystem;
    //每一个类型的文档总数
    private static HashMap<String,Integer> TEXT_NUM = new HashMap<>();

    static {
        try {
            fileSystem = FileSystem.get(new URI("hdfs://localhost:8020/"),configuration);
            getTextSize();
            evalution();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 统计测试集中每个类别的文本总数
     * @throws IOException
     */
    public static void getTextSize() throws IOException {
        FSDataInputStream textIn = fileSystem.open(new Path("/NBC/Evalution/output/part-r-00000"));
        BufferedReader textReader = new BufferedReader(new InputStreamReader(textIn));
        String line;
        while ((line=textReader.readLine())!=null){
            String[] data = line.split("\t");
            TEXT_NUM.put(data[0],Integer.parseInt(data[1]));
        }
        System.out.println("测试集中属于CHINA的文本数量是"+ TEXT_NUM.get("CHINA"));
        System.out.println("测试集中属于UK的文本数量是"+ TEXT_NUM.get("UK"));

    }

    /**
     * 建立每一个分类的混淆矩阵和全局混淆矩阵，计算相关评估指标
     */
    public static void evalution() throws IOException {
        double chinaTP = 0,chinaFN = 0,chinaFP = 0,chinaTN = 0;
        double ukTP = 0,ukFN = 0,ukFP = 0,ukTN = 0;
        FSDataInputStream textIn = fileSystem.open(new Path("/NBC/Test/output/part-r-00000"));
        BufferedReader textReader = new BufferedReader(new InputStreamReader(textIn));
        String line;
        while ((line=textReader.readLine())!=null){
            String[] data = line.split("\t");
            String truth = data[0].split("#")[1];
            String prediction = data[1];
            if(prediction.equals("CHINA")){
                if(truth.equals("UK")){
                    chinaFP++;
                }
            }else {
                if(truth.equals("CHINA")){
                    ukFP++;
                }
            }
            if(truth.equals("CHINA")){
                if(prediction.equals("CHINA")){
                    chinaTP++;
                    ukTN++;
                }else {
                    chinaFN++;
                }
            }else {
                if(prediction.equals("UK")){
                    ukTP++;
                    chinaTN++;
                }else {
                    ukFN++;
                }
            }
        }
        double chinaPrecision = (chinaTP)/(chinaTP+chinaFP);
        double chinaRecall = (chinaTP)/(chinaTP+chinaFN);
        double chinaF1 = (2*chinaPrecision*chinaRecall)/(chinaPrecision+chinaRecall);
        double ukPrecision = (ukTP)/(ukTP+ukFP);
        double ukRecall = (ukTP)/(ukTP+ukFN);
        double ukF1 = (2*ukPrecision*ukRecall)/(ukPrecision+ukRecall);
        System.out.println("CHINA的精确率，召回率，F1分别为"+chinaPrecision+" "+chinaRecall+" "+chinaF1);
        System.out.println();
        System.out.println("UK的精确率，召回率，F1分别为"+ukPrecision+" "+ukRecall+" "+ukF1);
        double microPrecision = (chinaTP+ukTP)/(chinaTP+ukTP+chinaFP+ukFP);
        double microRecall = (chinaTP+ukTP)/(chinaTP+ukTP+chinaFN+ukFN);
        double microF1 = (2*microPrecision*microRecall)/(microPrecision+microRecall);
        System.out.println();
        System.out.println("微平均的计算结果是:");
        System.out.println("精确率:"+microPrecision);
        System.out.println("召回率:"+microRecall);
        System.out.println("F1:"+microF1);
        double macroPrecision = (chinaPrecision+ukPrecision)/2;
        double macroRecall = (chinaRecall+ukRecall)/2;
        double macroF1 = (chinaF1+ukF1)/2;
        System.out.println();
        System.out.println("宏平均的计算结果是:");
        System.out.println("精确率:"+macroPrecision);
        System.out.println("召回率:"+macroRecall);
        System.out.println("F1:"+macroF1);

    }
    public static void main(String[] args) {
        
    }
}
