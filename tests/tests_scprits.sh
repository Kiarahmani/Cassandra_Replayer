deps=target/CrunchifySpringMVCFramework-0.0.1-SNAPSHOT-jar-with-dependencies.jar
serv () { cd /Users/Kiarash/dev/eclipse_workspace/Driver/target/classes; java sync.Scheduler; }
# 
lu3 () { java -cp $deps App select $1; }
lu1 () { java -cp $deps App init $1; }
lu2 () { java -cp $deps App increment $1; }
lu () { lu1 -1; wait; lu2 1 & lu2 2 & lu2 3 & lu2 4; wait; lu3 -1; }
#
dr1 () { java -cp $deps App init $1; }
dr2 () { java -cp $deps App double $1; }
dr3 () { java -cp $deps App select $1; }
dr () { dr1 -1; wait; dr2 1 & dr3 2; wait; }
