from google.cloud import storage
import os
import time
from datetime import datetime

# 设置服务账号密钥文件路径
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/rawdata/moobius-int-storage.json"

def download_with_retry(blob, local_file_path, max_retries=5):
    for attempt in range(max_retries):
        try:
            blob.download_to_filename(local_file_path)
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"下载失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                print("3秒后重试...")
                time.sleep(3)
            else:
                print(f"下载失败，已达到最大重试次数 ({max_retries}): {str(e)}")
                return False

def download_folder_from_gcs(bucket_name, source_folder, destination_folder):
    try:
        # 创建日志文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(destination_folder, f'download_log_{timestamp}.txt')
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        def log_message(message):
            print(message)
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"{message}\n")
        # 先获取所有blobs并转换为列表，这样可以多次遍历
        blobs = list(bucket.list_blobs(prefix=source_folder))
        
        # 获取云端的子文件夹列表
        cloud_subfolders = set()
        for blob in blobs:
            if blob.name.endswith('/'):
                subfolder = os.path.dirname(blob.name[len(source_folder):].rstrip('/'))
                if subfolder:  # 排除空字符串
                    cloud_subfolders.add(subfolder)
            else:
                subfolder = os.path.dirname(blob.name[len(source_folder):])
                if subfolder:  # 排除空字符串
                    cloud_subfolders.add(subfolder)

        # 过滤掉本地已存在的子文件夹
        folders_to_download = set()
        for subfolder in cloud_subfolders:
            local_subfolder_path = os.path.join(destination_folder, subfolder)
            if not os.path.exists(local_subfolder_path):
                folders_to_download.add(subfolder)
                log_message(f"将下载子文件夹: {subfolder}")
            else:
                log_message(f"子文件夹已存在，跳过下载: {subfolder}")

        if not folders_to_download:
            log_message("所有子文件夹都已存在，无需下载")
            return

        # 重新统计需要下载的文件
        total_files = 0
        total_folders = set()
        for blob in blobs:
            subfolder = os.path.dirname(blob.name[len(source_folder):])
            if subfolder in folders_to_download:
                if blob.name.endswith('/'):
                    folder_path = os.path.join(destination_folder, blob.name[len(source_folder):])
                    total_folders.add(folder_path)
                else:
                    total_files += 1
                    folder_path = os.path.dirname(os.path.join(destination_folder, blob.name[len(source_folder):]))
                    total_folders.add(folder_path)

        log_message(f"找到 {len(total_folders)} 个文件夹和 {total_files} 个文件")
        
        # 创建所有必要的文件夹
        for folder in sorted(total_folders):
            os.makedirs(folder, exist_ok=True)
            log_message(f"创建文件夹: {folder}")
        
        # 下载文件
        current_file = 0
        failed_downloads = []
        for blob in bucket.list_blobs(prefix=source_folder):
            if not blob.name.endswith('/'):
                current_file += 1
                relative_path = blob.name[len(source_folder):]
                local_file_path = os.path.join(destination_folder, relative_path)
                
                log_message(f"[{current_file}/{total_files}] 下载: {blob.name}")
                if download_with_retry(blob, local_file_path):
                    log_message(f"已保存到: {local_file_path}")
                else:
                    failed_downloads.append(blob.name)
        
        if failed_downloads:
            log_message("\n以下文件下载失败:")
            for file in failed_downloads:
                log_message(f"- {file}")
        else:
            log_message("\n所有文件下载成功!")
                
    except Exception as e:
        print(f"发生错误: {str(e)}")

def main():
    # 配置参数
    bucket_name = "yfd-bio"
    folders_to_download = ["textbook_ocr"]
    local_base_path = "/root/rawdata/gcs"  # 您可以修改这个保存路径
    
    # 创建基础下载目录
    os.makedirs(local_base_path, exist_ok=True)
    print(f"文件将下载到: {local_base_path}")
    
    # 下载每个文件夹
    for folder in folders_to_download:
        source_folder = f"{folder}/"
        destination_folder = os.path.join(local_base_path, folder)
        
        print(f"\n开始处理文件夹: {folder}")
        print(f"源路径: gs://{bucket_name}/{source_folder}")
        print(f"目标路径: {destination_folder}")
        
        download_folder_from_gcs(bucket_name, source_folder, destination_folder)
        print(f"完成文件夹 {folder} 的下载")

if __name__ == "__main__":
    main()