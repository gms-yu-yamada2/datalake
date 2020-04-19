import codecs
import boto3
import io
from boto3 import Session
from tqdm import tqdm
import gzip
import pandas as pd

s3client = Session().client('s3')


def get_all_keys(bucket: str = '', prefix: str = '', keys: [] = [], marker: str = '') -> [str]:
    """
    指定した prefix のすべての key の配列を返す
    """
    response = s3client.list_objects(
        Bucket=bucket, Prefix=prefix, Marker=marker)
    if 'Contents' in response:  # 該当する key がないと response に 'Contents' が含まれない
        keys.extend([content['Key'] for content in response['Contents']])
        if 'IsTruncated' in response:
            return get_all_keys(bucket=bucket, prefix=prefix, keys=keys, marker=keys[-1])
    return keys


def valid_gzip_format(obj):
    file = gzip.open(io.BytesIO(obj), 'rt')
    try:
        file.read()
        return True
    except OSError:
        return False


def ungzip(bucket, key, sw3):
    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )['Body'].read()
    file = gzip.open(io.BytesIO(obj), 'rt').read()
    return file


def convertgzip(bucket, key, outputbucket, outputkey):
    s3 = boto3.client('s3')

    print(bucket + ':' + key)
    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )['Body'].read()
    # print(obj)

    if valid_gzip_format(obj):
        file = gzip.open(io.BytesIO(obj), 'rt')
    else:
        raise InvalidCompressError('ファイル拡張子(.gz)に対して圧縮形式が正しくありません。')

    newcontents = file.read().replace('[', '').replace(']', '')
    newcontents = newcontents.replace('\\', '').replace(
        '"data":"{', '"data":{').replace('}",', '},').replace('"{}', '""')
    # print(newcontents)
    s3res = boto3.resource('s3')
    upobj = s3res.Object(bucket_name=outputbucket,
                         key=outkey)
    upobj.put(Body=newcontents)


# filename = '10000162_0 (1).gz'


def replaceText(filename):
    with gzip.open(filename, mode="rb") as f:  # 元ファイルがutf-8なので、バイナリで解凍
        newname = filename.replace(".gz", ".json")  # 解凍後のファイル名を作成
        reader = codecs.getreader("utf-8")  # codecsでutf-8で読込するオブジェクトを作成
        contents = reader(f)  # バイナリで開いたファイルオブジェクトをcodecsで読込 StreamReaderオブジェクト
        # 書込ファイルを作成するため、ファイルオブジェクトのnewfを作成

        newcontents = contents.read().replace(
            '"data":"{', '"data":[{').replace('}",', '}],')
        newcontents = newcontents.replace('\\', '').replace("{}", "")
        # print(newcontents)
        with open(newname, mode="w", encoding="utf-8", newline="\n") as newf:
            newf.write(newcontents)


if __name__ == "__main__":
    print("a")
    test_data = open("text_for_write 2.txt", "r")
    s3 = boto3.client('s3')
    s3res = boto3.resource('s3')

    bucket = "mspf-backup"
    keys = test_data.read().split('\n')
    outbucket = "past-mdata-test"
    outkey = "replaceData/"
    # print(keys)
    data = ''
    count = 0
    number = 0

    path_w = "data_list.txt"
    with open(path_w, mode='w') as f:
        for key in tqdm(keys):
            data = data + ungzip(bucket, key, s3)
            count += 1
            if count >= 100:
                data = data.replace('[', '').replace(']', '')
                data = data.replace('\\', '').replace(
                    '"data":"{', '"data":{').replace('}",', '},').replace('"{}', '""')
                upobj = s3res.Object(bucket_name=outbucket,
                                     key=outkey+str(number).zfill(5)+'.json')
                upobj.put(Body=data)
                count = 0
                number += 1
                data = ''
            f.writelines(key)
        if data != '':
            data = data.replace('[', '').replace(']', '')
            data = data.replace('\\', '').replace(
                '"data":"{', '"data":{').replace('}",', '},').replace('"{}', '""')
            upobj = s3res.Object(bucket_name=outbucket,
                                 key=outkey+str(number).zfill(5))
            upobj.put(Body=data)

    test_data.close()
