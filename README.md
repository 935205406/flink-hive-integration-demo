### 启动说明

flink1.10.0 与 hive 3.1.2 集成的文档说明不详细。
无论是启动flink集群还是在本地调试，都遇到许多问题。
整理如下：

---
##### flink集群启动：

lib目录需要加入一些jar包，以支持flink与hive的集成，官网文档给出的是Hive3.1.0的集成方案,
根据官网说明对Hive3.1.2做类似处理，启动集群就会报错。
经过错误排查，对加入的jar包进行了处理才解决问题

---
##### flink本地开发调试：

本地开发调试会遇到同样的问题。根据官网提示写的demo，运行会报错。
同样，将额外需要的jar包导入项目，scope 设置为 provided，启动时还需如下操作：

- 1.在项目名称上右键
- 2.选择 Open Module Setting (在倒数6个)
- 3.点击左侧 Libraries
- 4.org.apache.hive:hive-exec 依赖(是删除原生的hive-exec，我们打包后的 com.example.demo:hive-exec 不要删除)
- 5.运行项目即可 

因为hive是写入hdfs的，如果遇到hdfs写入权限问题，可以在启动前，在IDEA中配置项目启动的环境变量
HADOOP_USER_NAME=root

一定要在启动项目前，按步骤将hive-exec的依赖去除

---
##### hive-exec.jar 说明 

无论是flink集群启动还是本地开发调试，直接使用Hive 3.1.2自带的 hive-exec 会有问题，原因在于 hive-exec.jar 含有 com.google.common 的类，而这个版本与
hadoop 自带的 guava 版本不对，会报找不到类错误：
 >java.lang.NoSuchMethodError: com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;Ljava/lang/Object;)V

因此我们要对 hive-exec 重新打包，将jar包中的 com.google.common 重命名，使用 maven 的 shaded-jar 插件即可。
项目地址：https://github.com/935205406/hive-exec-shaded

---
##### 完整的介绍
关于flink1.10.0与hive3.1.2版本集成的完整介绍，可以参考:
https://www.jianshu.com/p/51d748fa3f48
