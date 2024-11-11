import os
import yaml
import shutil
import subprocess
from pathlib import Path
import json

# 这个脚本用于批量处理layout detection，只能与PDF-EXtract-Kit配合使用，并且需要提前修改一下yolo.py

# 基础配置
INPUT_ROOT = "/root/rawdata/gcs/textbook_images"
OUTPUT_ROOT = "/root/rawdata/gcs/textbook_images_detection"
CHECKPOINT_FILE = "layout_detection_progress.json"
CONFIG_TEMPLATE = "configs/layout_detection.yaml"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {'completed': [], 'failed': []}

def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def update_config(input_dir, output_dir):
    # 读取原始配置
    with open(CONFIG_TEMPLATE, 'r') as f:
        config = yaml.safe_load(f)
    
    # 更新配置
    config['inputs'] = input_dir
    config['outputs'] = output_dir
    
    # 保存临时配置文件
    temp_config = 'temp_layout_config.yaml'
    with open(temp_config, 'w') as f:
        yaml.dump(config, f)
    
    return temp_config

def main():
    # 加载检查点
    checkpoint = load_checkpoint()
    
    # 获取所有子文件夹
    subdirs = [d for d in os.listdir(INPUT_ROOT) 
              if os.path.isdir(os.path.join(INPUT_ROOT, d))]
    
    for subdir in subdirs:
        # 跳过已完成的文件夹
        if subdir in checkpoint['completed']:
            print(f"跳过已处理的文件夹: {subdir}")
            continue
            
        input_dir = os.path.join(INPUT_ROOT, subdir)
        output_dir = os.path.join(OUTPUT_ROOT, subdir)
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 更新配置文件
        temp_config = update_config(input_dir, output_dir)
        
        print(f"处理文件夹: {subdir}")
        try:
            # 执行layout detection
            subprocess.run([
                'python', 
                'scripts/layout_detection.py', 
                '--config', 
                temp_config
            ], check=True)
            
            # 更新检查点
            checkpoint['completed'].append(subdir)
            save_checkpoint(checkpoint)
        except Exception as e:
            print(f"处理文件夹 {subdir} 失败: {e}")
            # 更新检查点
            checkpoint['failed'].append(subdir)
            save_checkpoint(checkpoint)

if __name__ == "__main__":
    main() 