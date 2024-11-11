import fitz
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def convert_page(page_info):
    """单页转换函数"""
    pdf_path, page_num, output_dir, pdf_name = page_info
    
    pdf = fitz.open(pdf_path)
    page = pdf.load_page(page_num)
    
    # 获取页面尺寸
    rect = page.rect
    width_in_points = rect.width
    height_in_points = rect.height
    
    # 设置 DPI 和缩放
    target_dpi = 300
    zoom = target_dpi / 72.0
    
    # 创建高分辨率图片
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    
    # 计算当前 DPI
    width_in_pixels = pix.width
    height_in_pixels = pix.height
    dpi_w = math.floor((width_in_pixels / width_in_points) * 72)
    dpi_h = math.floor((height_in_pixels / height_in_points) * 72)
    
    # 保存图片
    output_path = f"{output_dir}/{pdf_name}_page_{page_num + 1}.png"
    pix.save(output_path)
    
    pdf.close()
    
    return {
        'page_num': page_num,
        'size': (width_in_pixels, height_in_pixels),
        'path': output_path
    }

def get_pdf_info_and_convert(pdf_path, output_dir):
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取PDF文件名（不含扩展名）
    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    
    print(f"处理文件: {pdf_path}")
    print(f"输出目录: {output_dir}")
    
    # 获取总页数
    pdf = fitz.open(pdf_path)
    total_pages = pdf.page_count
    pdf.close()
    
    print(f"PDF 总页数: {total_pages}")
    
    # 准备转换任务
    tasks = [(pdf_path, i, output_dir, pdf_name) for i in range(total_pages)]
    
    # 使用线程池执行转换
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 提交所有任务
        future_to_page = {executor.submit(convert_page, task): task[1] for task in tasks}
        
        # 处理完成的任务
        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                result = future.result()
                print(f"页面 {page_num + 1} 转换完成:")
                print(f"输出尺寸: {result['size'][0]} x {result['size'][1]} 像素")
                print(f"保存图片: {result['path']}\n")
            except Exception as e:
                print(f"页面 {page_num + 1} 转换失败: {str(e)}\n")
    
    print("转换完成！")

def process_pdf_directory(input_dir, output_base_dir):
    """处理指定目录下的所有PDF文件"""
    print(f"开始处理目录: {input_dir}")
    
    # 获取目录下所有PDF文件
    pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        print("未找到PDF文件！")
        return
    
    print(f"找到 {len(pdf_files)} 个PDF文件")
    
    # 处理每个PDF文件
    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_dir, pdf_file)
        pdf_name = os.path.splitext(pdf_file)[0]
        output_dir = os.path.join(output_base_dir, pdf_name)
        
        print(f"\n开始处理PDF文件: {pdf_file}")
        try:
            get_pdf_info_and_convert(pdf_path, output_dir)
        except Exception as e:
            print(f"处理 {pdf_file} 时发生错误: {str(e)}")
            continue

if __name__ == "__main__":
    # 设置输入和输出目录
    print("请输入PDF文件所在目录路径:")
    input_dir = input().strip()
    output_base_dir = "/root/rawdata/gcs/textbook_images"
    
    # 执行批量转换
    process_pdf_directory(input_dir, output_base_dir)