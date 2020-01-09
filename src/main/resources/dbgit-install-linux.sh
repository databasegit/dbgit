if type java ; then
   echo "Java found"
else
   echo "Downloading java"
   sudo apt-get install default-jre
fi

if type git ; then
   echo "Git found"
else
   echo "Downloading git"
   sudo apt-get install git
fi

USER_HOME=$HOME

echo "Copying files"
mkdir ~/dbgit -p
cp -r bin/ ~/dbgit
cp -r repo/ ~/dbgit
cp -r lang/ ~/dbgit
cp -r dbgitconfig ~/dbgit

cd ~/dbgit/bin
chmod +x dbgit

cd /usr/bin
sudo ln -sf $USER_HOME/dbgit/bin/dbgit

echo "Done!"