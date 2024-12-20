from google.cloud import storage
import os
import time
import json
from datetime import datetime
import humanize

# 设置服务账号密钥文件路径
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/tripletlu/downloads/moobius-int-storage.json"

def get_file_size(file_path):
    """获取文件大小的人类可读格式"""
    return humanize.naturalsize(os.path.getsize(file_path))

def load_progress():
    """加载上传进度"""
    try:
        with open('upload_progress.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {'uploaded_files': []}

def save_progress(uploaded_files):
    """保存上传进度"""
    with open('upload_progress.json', 'w') as f:
        json.dump({'uploaded_files': uploaded_files}, f)

def upload_with_retry(blob, local_file_path, max_retries=5):
    file_size = os.path.getsize(local_file_path)
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            blob.upload_from_filename(local_file_path)
            duration = time.time() - start_time
            speed = file_size / (1024 * 1024 * duration)  # MB/s
            print(f"上传速度: {speed:.2f} MB/s")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"上传失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                print("3秒后重试...")
                time.sleep(3)
            else:
                print(f"上传失败，已达到最大重试次数 ({max_retries}): {str(e)}")
                return False

def upload_folder_to_gcs(bucket_name, source_paths, destination_prefix):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # 加载之前的进度
        progress = load_progress()
        uploaded_files = set(progress['uploaded_files'])
        
        # 收集所有需要上传的文件
        file_list = []
        total_size = 0
        
        print("\n扫描文件...")
        for source_path in source_paths:
            if os.path.isdir(source_path):
                for root, _, files in os.walk(source_path):
                    for file in files:
                        local_path = os.path.join(root, file)
                        relative_path = os.path.relpath(local_path, os.path.dirname(source_path))
                        gcs_path = f"{destination_prefix}/{relative_path}".replace('\\', '/')
                        if local_path not in uploaded_files:
                            file_list.append((local_path, gcs_path))
                            total_size += os.path.getsize(local_path)
            else:
                if source_path not in uploaded_files:
                    gcs_path = f"{destination_prefix}/{os.path.basename(source_path)}".replace('\\', '/')
                    file_list.append((source_path, gcs_path))
                    total_size += os.path.getsize(source_path)
        
        print(f"找到 {len(file_list)} 个文件需要上传，总大小: {humanize.naturalsize(total_size)}")
        
        # 上传文件
        current_file = 0
        failed_uploads = []
        
        for local_path, gcs_path in file_list:
            current_file += 1
            file_size = get_file_size(local_path)
            print(f"\n[{current_file}/{len(file_list)}] 上传: {local_path}")
            print(f"文件大小: {file_size}")
            print(f"目标路径: gs://{bucket_name}/{gcs_path}")
            
            blob = bucket.blob(gcs_path)
            if upload_with_retry(blob, local_path):
                print(f"上传成功: {gcs_path}")
                uploaded_files.add(local_path)
                save_progress(list(uploaded_files))
            else:
                failed_uploads.append(local_path)
        
        if failed_uploads:
            print("\n以下文件上传失败:")
            for file in failed_uploads:
                print(f"- {file}")
        else:
            print("\n所有文件上传成功!")
                
    except Exception as e:
        print(f"发生错误: {str(e)}")

def main():
    # 配置参数
    bucket_name = "yfd-bio"
    source_paths = [
        "/home/tripletlu/downloads/rag_resources_backup/核心书库_figures_md_splits",
        "/home/tripletlu/downloads/rag_resources_backup/补充书库_figures_md_splits",
        "/home/tripletlu/downloads/rag_resources_backup/核心书库_figures_md.gz",
        "/home/tripletlu/downloads/rag_resources_backup/补充书库_figures_md.gz",
        "/home/tripletlu/downloads/rag_resources_backup/spliter.py"
    ]
    destination_prefix = "cleaned_figures_gz"
    
    print(f"\n开始上传任务")
    print(f"目标路径: gs://{bucket_name}/{destination_prefix}")
    
    upload_folder_to_gcs(bucket_name, source_paths, destination_prefix)
    print(f"上传任务完成")

if __name__ == "__main__":
    main()
