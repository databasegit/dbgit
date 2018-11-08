link jdbc:postgresql://localhost/test user=postgres password=postgres
status
add schema1.sch
dump -a


link for Oracle - link jdbc:oracle:thin:@localhost:1521:DBGIT user=oracle password=oracle

Как положить Oracle JDBC Driver в локальный (на компьютере) репозиторий Maven (Перед началом работы - должен быть установлен MAVEN_HOME для доступа к командам Мавена):

1) Скачать jar-файл по ссылке (https://www.oracle.com/content/secure/maven/content/com/oracle/jdbc/ojdbc8/18.3.0.0/ojdbc8-18.3.0.0.jar), потребуется Oracle Account

2) Выполнить команду, указав путь до jar-файла 
	mvn install:install-file -DgroupId=com.oracle.jdbc -DartifactId=ojdbc8 -Dversion=18.3.0.0 -Dpackaging=jar -Dfile=<Path to file>/ojdbc8-18.3.0.0.jar -DgeneratePom=true

(Опционально) Если в POM-файле проекта нет указания на Oracle JDBC Drive, то добавить
	<dependency>
 		<groupId>com.oracle.jdbc</groupId>
		<artifactId>ojdbc8</artifactId>
		<version>18.3.0.0</version>
	</dependency>

P.S. Если нужен оригинальный pom, то он доступен по ссылке (https://www.oracle.com/content/secure/maven/content/com/oracle/jdbc/ojdbc8/18.3.0.0/ojdbc8-18.3.0.0.pom)