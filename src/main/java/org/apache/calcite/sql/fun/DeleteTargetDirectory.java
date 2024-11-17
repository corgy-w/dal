package org.apache.calcite.sql.fun;

import java.io.File;

public class DeleteTargetDirectory {
    public static void main(String[] args) {
        // 设置根目录路径
        String rootPath = "/Users/coolcorgy/zhongan";
        
        // 创建File对象
        File rootDir = new File(rootPath);
        
        // 检查根目录是否存在且为目录
        if (rootDir.exists() && rootDir.isDirectory()) {
            // 调用方法删除名为target的子目录
            deleteTargetDirectories(rootDir, "target");
        } else {
            System.out.println("指定路径不是有效目录: " + rootPath);
        }
    }

    /**
     * 删除目录下名为指定名称的子目录
     *
     * @param directory 要扫描的目录
     * @param targetName 要删除的子目录名称
     */
    private static void deleteTargetDirectories(File directory, String targetName) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // 检查是否为目标目录
                    if (file.getName().equals(targetName)) {
                        deleteDirectory(file);
                        System.out.println("已删除目录: " + file.getAbsolutePath());
                    } else {
                        // 递归检查子目录
                        deleteTargetDirectories(file, targetName);
                    }
                }
            }
        }
    }

    /**
     * 删除整个目录及其内容
     *
     * @param directory 要删除的目录
     * @return 删除是否成功
     */
    private static boolean deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        System.out.println("删除文件" + directory.getAbsolutePath());
        return directory.delete();
    }
}
