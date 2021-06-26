import boto3

KEY = 'AKIAWYGOB3UEY32CSTCF' 
SECRET = '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'

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )

sampleDbBucket =  s3.Bucket("udacity-dend")

print(sampleDbBucket.objects)
# for obj in sampleDbBucket.objects.filter(Prefix="log_data"):
for obj in sampleDbBucket.objects:
    print(obj)