rm *.apk -rf
rm wget_log -rf
rm derby.log -rf
rm celerybeat* -rf
rm -rf .idea
git rm *.pyc
git commit -m 'commit'
git push
git add .
git commit -m 'commit'
git push

