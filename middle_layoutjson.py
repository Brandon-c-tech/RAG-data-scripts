import json
import os

def split_json_by_pages(input_file, output_dir, pages_per_file):
    print(f"开始处理文件: {input_file}")
    print(f"输出目录: {output_dir}")
    print(f"每个文件页数: {pages_per_file}")
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取原始JSON
    print("正在读取JSON文件...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 获取页面列表
    pages = data['pdf_info']
    total_pages = len(pages)
    print(f"总页数: {total_pages}")
    
    # 按每个文件页数进行分割
    for i in range(0, total_pages, pages_per_file):
        chunk = {
            'pdf_info': pages[i:i + pages_per_file]
        }
        
        # 生成输出文件名
        output_file = os.path.join(
            output_dir, 
            f'pages_{i+1}_to_{min(i+pages_per_file, total_pages)}.json'
        )
        
        print(f"正在保存文件: {output_file}")
        # 保存分割后的文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)
    
    print("文件处理完成!")


if __name__ == '__main__':
    # 假设原文件路径
    input_path = '/root/rawdata/gcs/textbook_ocr/6 细胞生物学（5）/auto/6 细胞生物学（5）_middle.json'
    output_dir = '/root/rawdata/json_split'

    # 拆分文件
    split_json_by_pages(input_path, output_dir, pages_per_file=3)