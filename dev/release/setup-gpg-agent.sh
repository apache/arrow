# source me
eval $(gpg-agent --daemon --allow-preset-passphrase)
gpg --use-agent -s LICENSE.txt 
