package cn;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class test {
    static public void main(String[] args) throws IOException {

        //源数据根目录
        String root = "D:\\1\\UCRArchive_2018";

        //目标数据根目录
        String targetRoot = "D:\\tsfile\\UCRArchive_2018_TS";
        for (String path : getFilePaths(root)) {
            String name = new File(path).getName();
            String targetPath = targetRoot + "\\" + name;
            new File(targetPath).mkdirs();
            for (String file : getFiles(path)) {
                if (file.endsWith(".tsv")) {
                    File sourceFile = new File(file);
                    HashMap<String, List<float[]>> data = readTSV(file);
                    String targetFilePath = targetPath + "\\" + sourceFile.getName().replace(".tsv", ".ts");
                    File targetTsFile = new File(targetFilePath);
                    if(targetTsFile.exists()){
                        targetTsFile.delete();
                    }
                    targetTsFile.createNewFile();
                    writeTsFile(data, targetFilePath);
                }
            }
        }
    }

    static public void writeTsFile(HashMap<String, List<float[]>> data, String filePath) {
        try {
            String s = "{\n" +
                    "    \"schema\": [\n" +
                    "        {\n" +
                    "            \"measurement_id\": \"sensor_1\",\n" +
                    "            \"data_type\": \"FLOAT\",\n" +
                    "            \"encoding\": \"RLE\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"measurement_id\": \"sensor_2\",\n" +
                    "            \"data_type\": \"INT32\",\n" +
                    "            \"encoding\": \"TS_2DIFF\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"measurement_id\": \"sensor_3\",\n" +
                    "            \"data_type\": \"INT32\",\n" +
                    "            \"encoding\": \"TS_2DIFF\"\n" +
                    "        }\n" +
                    "    ],\n" +
                    "    \"row_group_size\": 134217728\n" +
                    "}";
            JSONObject schemaObject = new JSONObject(s);
            JSONArray schema = new JSONArray();
            for (String key : data.keySet()) {
                for (int i = 0; i < data.get(key).size(); i++) {
                    JSONObject j = new JSONObject();
                    j.put("measurement_id", "sensor_" + i);
                    j.put("data_type", "FLOAT");
                    j.put("encoding", "RLE");
                    try {
                        schema.get(i);
                    } catch (Exception e) {
                        schema.put(j);
                    }
                }
            }
            schemaObject.put("schema", schema);

            TsRandomAccessFileWriter output = new TsRandomAccessFileWriter(new File(filePath));
            TsFile tsFile = new TsFile(output, schemaObject);

            for (String deviceIndex : data.keySet()){
                List<float[]> lt = data.get(deviceIndex);
                ArrayList<DataPoint>[] dataPointLists = new ArrayList[lt.get(0).length];
                for(int i=0;i<dataPointLists.length;i++){
                    dataPointLists[i] = new ArrayList<>();
                }
                int sensorIndex = 0;
                for (float[] fs : lt){
                    int time = 0;
                    for(float f:fs){
                        dataPointLists[time].add(new FloatDataPoint("sensor_" + sensorIndex, f));
                        time++;
                    }
                    sensorIndex++;
                }

                int time = 0;
                for(ArrayList<DataPoint> d:dataPointLists) {
                    TSRecord tsRecord = new TSRecord(time, "device_" + deviceIndex);
                    tsRecord.dataPointList = d;
                    tsFile.writeRecord(tsRecord);
                    time++;
                }
            }

            tsFile.close();
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    static public List<String> getFilePaths(String path) {
        List<String> filePaths = new ArrayList<>();
        File file = new File(path);
        File[] tempList = file.listFiles();
        assert tempList != null;
        for (File value : tempList) {
            if (value.isDirectory()) {
//              System.out.println("文件夹：" + tempList[i]);
                filePaths.add(value.toString());
            }
        }
        return filePaths;
    }

    static public List<String> getFiles(String path) {
        List<String> files = new ArrayList<>();
        File file = new File(path);
        File[] tempList = file.listFiles();
        assert tempList != null;
        for (File value : tempList) {
            if (value.isFile()) {
//              System.out.println("文     件：" + tempList[i]);
                files.add(value.toString());
            }
        }
        return files;
    }

    static public HashMap<String, List<float[]>> readTSV(String path) throws IOException {
        TsvParserSettings settings = new TsvParserSettings();
        settings.setMaxColumns(10000);
        settings.getFormat().setLineSeparator("\n");
        TsvParser parser = new TsvParser(settings);
        FileInputStream stream = new FileInputStream(path);
        List<String[]> allRows = parser.parseAll(stream);
        HashMap<String, List<float[]>> data = new HashMap<>();
        for (String[] allRow : allRows) {
            String device = allRow[0];
            data.computeIfAbsent(device, k -> new ArrayList<>());
            List<float[]> lt = data.get(device);
            int i = 0;
            float[] row_data_t = new float[allRow.length - 1];
            for (String f : allRow) {
                if (i > 0) {
                    row_data_t[i - 1] = Float.parseFloat(f);
                }
                i++;
            }
            lt.add(row_data_t);
            data.replace(device, lt);
        }
        parser.stopParsing();
        stream.close();
        return data;
    }

}
