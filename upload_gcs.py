from google.cloud import storage
import os
import time

# 设置服务账号密钥文件路径
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/rawdata/moobius-int-storage.json"

def upload_with_retry(blob, local_file_path, max_retries=5):
    for attempt in range(max_retries):
        try:
            blob.upload_from_filename(local_file_path)
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"上传失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                print("3秒后重试...")
                time.sleep(3)
            else:
                print(f"上传失败，已达到最大重试次数 ({max_retries}): {str(e)}")
                return False

def should_upload_file(file_path, folder_name, base_folder):
    """判断文件是否需要上传"""
    # 构建需要匹配的完整路径
    json_file = os.path.join(base_folder, folder_name, 'auto', f'{folder_name}_figures_description.json')
    
    # 检查是否是指定的 json 文件
    if file_path == json_file:
        return True
    
    return False

def upload_folder_to_gcs(bucket_name, source_folder, destination_prefix):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # 统计要上传的文件
        total_files = 0
        file_list = []
        folder_name = os.path.basename(source_folder)
        base_folder = '/root/rawdata/gcs/textbook_ocr'
        print(f"\n扫描目录 {source_folder} ...")
        
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                local_path = os.path.join(root, file)
                # 检查文件是否需要上传
                if should_upload_file(local_path, folder_name, base_folder):
                    # 构建 GCS 路径：移除基础路径部分，保留子文件夹之后的路径
                    relative_path = local_path.replace(os.path.join(base_folder, folder_name) + '/', '')
                    gcs_path = os.path.join(destination_prefix, relative_path).replace('\\', '/')
                    file_list.append((local_path, gcs_path))
                    total_files += 1
        
        print(f"找到 {total_files} 个文件需要上传")
        
        # 上传文件
        current_file = 0
        failed_uploads = []
        
        for local_path, gcs_path in file_list:
            current_file += 1
            print(f"[{current_file}/{total_files}] 上传: {local_path}")
            print(f"目标路径: gs://{bucket_name}/{gcs_path}")
            
            blob = bucket.blob(gcs_path)
            if upload_with_retry(blob, local_path):
                print(f"上传成功: {gcs_path}")
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
    base_path = '/root/rawdata/gcs/textbook_ocr'
    source_folders = [os.path.join(base_path, folder) for folder in os.listdir(base_path) 
                     if os.path.isdir(os.path.join(base_path, folder))]

    # 上传每个文件夹
    for folder in source_folders:
        folder_name = os.path.basename(folder)
        destination_prefix = f"rag_resources/{folder_name}"
        
        print(f"\n开始处理文件夹: {folder_name}")
        print(f"源路径: {folder}")
        print(f"目标路径: gs://{bucket_name}/{destination_prefix}")
        
        upload_folder_to_gcs(bucket_name, folder, destination_prefix)
        print(f"完成文件夹 {folder_name} 的上传")

if __name__ == "__main__":
    main()