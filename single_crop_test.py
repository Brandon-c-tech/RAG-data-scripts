import fitz
import argparse
from pathlib import Path

def get_max_bbox(bboxes):
    """
    计算多个bbox的最大矩形范围
    
    参数:
        bboxes: bbox列表，每个bbox格式为 [x0, y0, x1, y1]
    返回:
        最大矩形范围 [x0, y0, x1, y1]
    """
    if not bboxes:
        return None
        
    max_bbox = [
        min(b[0] for b in bboxes),  # x0
        min(b[1] for b in bboxes),  # y0 
        max(b[2] for b in bboxes),  # x1
        max(b[3] for b in bboxes)   # y1
    ]
    return max_bbox

def crop_pdf_page(pdf_path, page_num, bbox, output_dir="debug_crops"):
    """
    从PDF页面裁剪指定区域并保存为高清图片
    
    参数:
        pdf_path: PDF文件路径
        page_num: 页码 (从0开始)
        bbox: 裁剪区域 [x0, y0, x1, y1]
        output_dir: 输出目录
    """
    # 创建输出目录
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 打开PDF
    doc = fitz.open(pdf_path)
    try:
        # 获取指定页面
        page = doc[page_num]
        
        # 设置裁剪区域
        clip = fitz.Rect(bbox)
        
        # 设置600 DPI的缩放
        zoom = 600 / 72
        matrix = fitz.Matrix(zoom, zoom)
        
        # 生成图片
        pix = page.get_pixmap(matrix=matrix, clip=clip, alpha=False)
        
        # 生成输出文件名
        output_file = output_path / f"TEST_page_{page_num + 1}_bbox_{'-'.join(map(str, bbox))}.png"
        
        # 保存图片
        pix.save(str(output_file))
        print(f"已保存图片到: {output_file}")
        
    finally:
        doc.close()

def main():
    # 硬编码输入参数
    pdf_path = "/root/rawdata/gcs/textbook_ocr/6 细胞生物学（5）/auto/6 细胞生物学（5）_layout.pdf"
    page_num = 25  # 从0开始的页码
    bbox = get_max_bbox([[
            1,
            82,
            246,
            95
          ],[
            1,
            1,
            215,
            73
          ]])
    
    
    
    # 裁剪区域坐标 [x0, y0, x1, y1]
    output_dir = "/root/rawdata/single_output_test"  # 输出目录
    
    crop_pdf_page(pdf_path, page_num - 1, bbox, output_dir)

if __name__ == "__main__":
    main()