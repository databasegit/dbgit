if type java ; then
   echo "Java found"
else
   echo "Downloading java"
   apt-get install default-jre
fi

if type git ; then
   echo "Git found"
else
   echo "Downloading git"
   apt-get install git
fi

echo "Copying files"
mkdir ~/dbgit -p
cp -r bin/ ~/dbgit
cp -r repo/ ~/dbgit

cd ~/dbgit/bin
chmod +x dbgit

cd /usr/bin
ln -sf ~/dbgit/bin/dbgit

echo "Done!