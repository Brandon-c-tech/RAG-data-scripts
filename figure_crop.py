# 这个脚本用于从pdf中提取图片和表格，并保存为png图片

import json
import fitz  # PyMuPDF
import os
from pathlib import Path
import hashlib
import logging
from datetime import datetime

def setup_logging():
    """设置日志配置"""
    log_dir = "/root/rawdata"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"figure_crop_{timestamp}.log")
    
    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    return logging.getLogger(__name__)

class PDFElementExtractor:
    def __init__(self, pdf_path, layout_json_path, output_dir):
        print(f"初始化PDF提取器...")
        self.pdf_doc = fitz.open(pdf_path)
        self.layout_json_path = layout_json_path
        
        # 提取 PDF 文件名
        self.pdf_name = Path(pdf_path).stem  # 获取文件名（不带扩展名）
        
        with open(layout_json_path, 'r', encoding='utf-8') as f:
            self.layout_data = json.load(f)
            
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def extract_elements(self):
        """提取所有图片和表格"""
        print("开始提取元素...")
        
        def extract_bboxes_recursive(data):
            bboxes = []
            
            # 基本情况:如果是非列表或字典类型,直接返回空列表
            if not isinstance(data, (list, dict)):
                return []
                
            # 如果是列表,递归处理每个元素
            if isinstance(data, list):
                for item in data:
                    bboxes.extend(extract_bboxes_recursive(item))
                    
            # 如果是字典
            if isinstance(data, dict):
                if 'bbox' in data:
                    bboxes.append(data['bbox'])
                    
                # 递归处理字典的所有值
                for value in data.values():
                    bboxes.extend(extract_bboxes_recursive(value))
                    
            return bboxes

        # 获取pdf_info数组
        pdf_info = self.layout_data.get('pdf_info', [])
        
        for page_data in pdf_info:
            page_idx = int(page_data.get('page_idx', 0))
            pdf_page = self.pdf_doc[page_idx]
            print(f"处理第 {page_idx} 页")
            
            all_bboxes = []

            # 遍历preproc_blocks
            for block in page_data.get('preproc_blocks', []):
                block_type = block.get('type')
                if block_type in ['image', 'table']:
                    print(f"发现 {block_type}...")
                    # 对这个block开始递归提取bbox
                    extracted_bboxes = extract_bboxes_recursive(block)
                    
                    # 添加验证确保bbox格式正确
                    valid_bboxes = []
                    for bbox in extracted_bboxes:
                        if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                            valid_bboxes.append(bbox)
                        else:
                            print(f"警告：跳过无效的bbox格式: {bbox}")
                    
                    all_bboxes.extend(valid_bboxes)
                    
                    # 检查是否有有效的bbox
                    if valid_bboxes:
                        max_bbox = [
                            min(b[0] for b in valid_bboxes),  # x0
                            min(b[1] for b in valid_bboxes),  # y0
                            max(b[2] for b in valid_bboxes),  # x1
                            max(b[3] for b in valid_bboxes)   # y1
                        ]
                        print(f"最大矩形的bbox: {max_bbox}")
                        
                        metadata = {
                            'type': block_type,
                            'page_num': f"page_{page_idx + 1}",
                            'bbox': max_bbox,  # 使用计算出的最大bbox
                            'index': block.get('index', 0)
                        }
                        
                        # 生成截图
                        clip = fitz.Rect(max_bbox)  # 使用最大bbox进行截图
                        # 设置缩放因子以实现600 DPI
                        zoom_x = 600 / 72  # 水平缩放因子
                        zoom_y = 600 / 72  # 垂直缩放因子
                        matrix = fitz.Matrix(zoom_x, zoom_y)  # 创建矩阵
                        pix = pdf_page.get_pixmap(matrix=matrix, clip=clip, alpha=False)  # 使用矩阵生成图片
                        
                        # 生成文件名并保存
                        filename_base = self._generate_filename(metadata)
                        image_path = self.output_dir / f"{filename_base}.png"
                        pix.save(str(image_path))
                        print(f"保存图片: {image_path}")
                    else:
                        print(f"警告：在{block_type}中没有找到有效的bbox")
                        continue

    def close(self):
        """关闭PDF文档"""
        self.pdf_doc.close()

    def _generate_filename(self, metadata):
        """生成唯一的文件名"""
        type_str = "图" if metadata['type'] == 'image' else "表"
        filename = f"{self.pdf_name}_{metadata['page_num']}_{type_str}_{metadata['index']}"
        return filename

def main():
    # 设置日志
    logger = setup_logging()
    logger.info("开始处理PDF文件提取任务")
    
    # 基础路径
    base_dir = "/root/rawdata/gcs/textbook_ocr"
    
    # 遍历textbook_ocr下的所有子文件夹
    for subdir in os.listdir(base_dir):
        subdir_path = os.path.join(base_dir, subdir)
        if not os.path.isdir(subdir_path):
            continue
            
        # 构建文件路径
        auto_dir = os.path.join(subdir_path, "auto")
        if not os.path.exists(auto_dir):
            logger.warning(f"跳过 {subdir}: 没有找到auto目录")
            continue
            
        pdf_path = os.path.join(auto_dir, f"{subdir}_origin.pdf")
        layout_json_path = os.path.join(auto_dir, f"{subdir}_middle.json")
        output_dir = os.path.join(auto_dir, "figures")
        
        # 检查必要文件是否存在
        if not os.path.exists(pdf_path) or not os.path.exists(layout_json_path):
            logger.warning(f"跳过 {subdir}: PDF或JSON文件不存在")
            continue
            
        logger.info(f"处理文件夹: {subdir}")
        logger.info(f"PDF路径: {pdf_path}")
        logger.info(f"JSON路径: {layout_json_path}")
        logger.info(f"输出目录: {output_dir}")
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 创建提取器并处理
        extractor = PDFElementExtractor(pdf_path, layout_json_path, output_dir)
        try:
            extractor.extract_elements()
            logger.info(f"成功处理完成: {subdir}")
        except Exception as e:
            logger.error(f"处理 {subdir} 时发生错误: {str(e)}", exc_info=True)
        finally:
            extractor.close()
    
    logger.info("所有PDF处理任务完成")

if __name__ == "__main__":
    main()