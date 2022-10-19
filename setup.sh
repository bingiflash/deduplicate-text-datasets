yum update -y

yum -y install gcc-c++

curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install transformers s3urls boto3 datasets

cargo build