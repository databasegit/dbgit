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
mkdir /var/dbgit
cp dbgit.jar /var/dbgit
cp -r lib/ /var/dbgit

cp dbgit /usr/bin
cd /usr/bin
chmod +x dbgit

echo "Done!"