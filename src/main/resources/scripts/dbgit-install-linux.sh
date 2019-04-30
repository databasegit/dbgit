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
mkdir /var/dbgit -p
cp -r bin/ /var/dbgit
cp -r repo/ /var/dbgit

chmod +x dbgit

cd /usr/bin
ln -s /var/dbgit/bin/dbgit

echo "Done!"