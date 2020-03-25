# 自定义Count-window实现demo
```
git clone https://github.com/popvlovs/Esper-demo.git

mvn clean package -DskipTests -e -U

java \
-Xmx43648M -Xms43648M \
-XX:MaxMetaspaceSize=512M -XX:MetaspaceSize=512M \
-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled \
-classpath esper6-cep-demo-0.1.jaespercep.demo.cases.WindowCountMetric_Multithread_7
```
