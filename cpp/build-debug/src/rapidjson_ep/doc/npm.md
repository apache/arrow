## NPM

# package.json {#package}

~~~~~~~~~~js
{
  ...
  "dependencies": {
    ...
    "rapidjson": "git@github.com:Tencent/rapidjson.git"
  },
  ...
  "gypfile": true
}
~~~~~~~~~~

# binding.gyp {#binding}

~~~~~~~~~~js
{
  ...
  'targets': [
    {
      ...
      'include_dirs': [
        '<!(node -e \'require("rapidjson")\')'
      ]
    }
  ]
}
~~~~~~~~~~
