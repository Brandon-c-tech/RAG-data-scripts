
# 因为base64编码会让文件体积变得巨大，这个脚本的方案不太能用

import asyncio
import base64
import json
from pathlib import Path
from typing import List

async def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def find_figure_dirs(root_dir: str) -> List[Path]:
    """查找所有包含 figures 文件夹的目录"""
    root_path = Path(root_dir)
    return list(root_path.rglob('figures'))

async def generate_request(image_path: Path) -> dict:
    """为单个图片生成请求格式"""
    base64_image = await encode_image(str(image_path))
    
    prompt = '''
        请详细描述这张科学教学示意图。要求：

        1. 描述长度：控制在100-300字之间

        2. 描述结构（请按以下顺序组织内容）：
        A. 开篇概述（15-25字）：
            - 说明图示的主要生物学概念/原理
            - 点明图示类型（如截面图、流程图、结构图等）
        
        B. 核心内容（50-150字）：
            - 详细解释图中展示的生物学原理或概念
            - 描述关键组成部分及其关系
            - 说明重要的因果关系或变化过程
            - 解释图中的箭头、标注等视觉元素含义
        
        C. 教学功能（20-50字）：
            - 说明该图在教学中的作用
            - 指出图示帮助理解的关键点
        
        D. 补充说明（如有必要，15-75字）：
            - 相关的生物学应用场景
            - 与其他生物学概念的联系
            - 特殊的注意事项

        3. 描述原则：
        - 使用专业准确的生物学术语
        - 保持逻辑性和连贯性
        - 由表及里，由简到繁
        - 注重概念间的关联性
        
        4. 语言要求：
        - 使用生物学教材的规范表述
        - 避免过于口语化的表达
        - 必要时使用专业术语
        - 保持客观严谨的语气

        5. 特别注意：
        - 不要使用"这是一张..."等开场白
        - 不评价图片的设计质量
        - 如涉及步骤或过程，要清晰标明顺序
        - 如有数值或单位，需准确描述
        - 如有图例或备注，要包含在描述中
    '''

    return {
        "custom_id": image_path.stem,  # 使用文件名作为 custom_id
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o-mini",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 1000
        }
    }

async def process_directory(output_base_path: str, batch_size: int = 40):
    """处理目录下的图片并输出到多个jsonl文件，每个文件包含指定数量的图片"""
    image_extensions = {'.jpg', '.jpeg', '.png'}
    root_dir = "/root/rawdata/gcs/textbook_ocr/1 普通生物学（5）"
    figure_dirs = find_figure_dirs(root_dir)
    
    current_file_num = 1
    current_batch_count = 0
    current_file = None
    
    def get_output_file(base_path: str, num: int) -> str:
        """生成输出文件路径"""
        path = Path(base_path)
        return str(path.parent / f"{path.stem}_{num}{path.suffix}")
    
    try:
        current_file = open(get_output_file(output_base_path, current_file_num), 'w', encoding='utf-8')
        
        for figure_dir in figure_dirs:
            image_files = [f for f in figure_dir.iterdir() if f.suffix.lower() in image_extensions]
            
            for image_path in image_files:
                # 如果当前批次已满，创建新文件
                if current_batch_count >= batch_size:
                    current_file.close()
                    current_file_num += 1
                    current_file = open(get_output_file(output_base_path, current_file_num), 'w', encoding='utf-8')
                    current_batch_count = 0
                
                request = await generate_request(image_path)
                json_line = json.dumps(request, ensure_ascii=False) + '\n'
                current_file.write(json_line)
                current_batch_count += 1
                print(f"已处理: {image_path} -> batch_{current_file_num}.jsonl ({current_batch_count}/{batch_size})")
    
    finally:
        if current_file:
            current_file.close()

async def main():
    output_base_path = "/root/rawdata/batch_request/batch_requests.jsonl"
    print(f"开始处理图片...")
    await process_directory(output_base_path)
    print(f"处理完成")

if __name__ == "__main__":
    asyncio.run(main())