[ -e abs-housing-package.zip ] && rm abs-housing-package.zip
[ -d package ] && rm -r package
pip install --target ./package requests
cd package
zip -r ../abs-housing-package.zip .
cd ..
zip -g abs-housing-package.zip abs_housing.py
aws s3 cp abs-housing-package.zip s3://code-deployment-abs/abs-housing-package.zip --profile schoolratings