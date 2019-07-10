
CURRENT_DIR=$PWD


echo $CURRENT_DIR

if /Library/Internet\ Plug-Ins/JavaAppletPlugin.plugin/Contents/Home/bin/java -version ; then 
echo «Java found» 
else 
echo «Downloading java» 

curl -jkL -H "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-macosx-x64.dmg -o jdk-8u131-macosx-x64.dmg

hdiutil attach jdk-8u131-macosx-x64.dmg

cd "/Volumes/JDK 8 Update 131"
sudo installer -pkg "JDK 8 Update 131.pkg" -target "/"
cd $CURRENT_DIR
hdiutil detach "/Volumes/JDK 8 Update 131"

fi

if type /usr/local/bin/git ;
then
echo «git found»
else
echo «downloading git»
curl -O https://netix.dl.sourceforge.net/project/git-osx-installer/git-2.6.2-intel-universal-mavericks.dmg

hdiutil attach git-2.6.2-intel-universal-mavericks.dmg

cd "/Volumes/Git 2.6.2 Mavericks Intel Universal"
sudo installer -pkg git-2.6.2-intel-universal-mavericks.pkg -target "/"
cd $CURRENT_DIR
hdiutil detach "/Volumes/Git 2.6.2 Mavericks Intel Universal"

fi

USER_HOME=$HOME

echo "copying files"
mkdir -p ~/dbgit
cp -r bin/ ~/dbgit/bin
cp -r repo/ ~/dbgit/repo

cd ~/dbgit/bin
chmod +x dbgit

Sudo mkdir -p /usr/local/bin
sudo ln -sf $USER_HOME/dbgit/bin/dbgit /usr/local/bin/dbgit

echo "Done!"

