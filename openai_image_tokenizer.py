import os
from PIL import Image
import math

# 这个脚本用于计算图片的token数量，用于OpenAI API的调用限制
# 算出来不准还是怎么了，最后坑死了我，实际上搞一本书大概要四美元

def calculate_high_detail_tokens(width, height):
    # 首先确保尺寸在2048x2048范围内
    if width > 2048 or height > 2048:
        ratio = 2048 / max(width, height)
        width = int(width * ratio)
        height = int(height * ratio)
    
    # 将最短边缩放到768px
    shortest_side = min(width, height)
    if shortest_side > 768:
        ratio = 768 / shortest_side
        width = int(width * ratio)
        height = int(height * ratio)
    
    # 计算需要多少个512px的方块
    tiles_width = math.ceil(width / 512)
    tiles_height = math.ceil(height / 512)
    total_tiles = tiles_width * tiles_height
    
    # 计算总token：每个tile 170 tokens + 基础85 tokens
    return (total_tiles * 170) + 85

def calculate_directory_tokens(directory_path):
    total_high_detail_tokens = 0
    total_low_detail_tokens = 0
    file_count = 0
    
    for filename in os.listdir(directory_path):
        if filename.lower().endswith('.png'):
            file_path = os.path.join(directory_path, filename)
            try:
                with Image.open(file_path) as img:
                    width, height = img.size
                    high_detail_tokens = calculate_high_detail_tokens(width, height)
                    low_detail_tokens = 85  # 固定值
                    
                    print(f"\n图片: {filename}")
                    print(f"尺寸: {width}x{height}")
                    print(f"High detail tokens: {high_detail_tokens}")
                    print(f"Low detail tokens: {low_detail_tokens}")
                    
                    total_high_detail_tokens += high_detail_tokens
                    total_low_detail_tokens += low_detail_tokens
                    file_count += 1
                    
            except Exception as e:
                print(f"处理 {filename} 时出错: {str(e)}")
    
    print(f"\n总计:")
    print(f"处理的PNG文件数量: {file_count}")
    print(f"High detail 模式总 tokens: {total_high_detail_tokens}")
    print(f"Low detail 模式总 tokens: {total_low_detail_tokens}")

# 使用示例
if __name__ == "__main__":
    directory = "/root/rawdata/test_output"
    if os.path.exists(directory):
        calculate_directory_tokens(directory)
    else:
        print("目录不存在！")
