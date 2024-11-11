import asyncio
import base64
import json
import os
from pathlib import Path
from openai import AsyncOpenAI
from typing import Dict
import time
import glob

# 使用gpt-4o-mini模型，通过OpenAI API对科学教学图片进行智能描述。
# 该脚本会读取指定目录下的图片，调用API生成规范的教学图片描述文本。
# 描述文本包含开篇概述、核心内容、教学功能和补充说明等结构化内容。

client = AsyncOpenAI()

async def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

async def get_image_description(image_path: str) -> str:
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

    base64_image = await encode_image(image_path)
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
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
            max_tokens=1000
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"处理图片 {image_path} 时出错: {str(e)}")
        return ""

async def process_images(image_dir: str, output_path: str, folder_name: str) -> Dict[str, str]:
    # 检查checkpoint文件是否存在
    output_file = Path(output_path) / f"{folder_name}_figures_description.json"
    existing_descriptions = {}
    
    if output_file.exists():
        with open(output_file, 'r', encoding='utf-8') as f:
            existing_descriptions = json.load(f)
            print(f"找到已存在的处理结果，已处理{len(existing_descriptions)}张图片")
    
    # 支持的图片格式
    image_extensions = {'.jpg', '.jpeg', '.png'}
    descriptions = existing_descriptions.copy()
    
    # 获取所有图片文件
    image_files = [
        f for f in Path(image_dir).iterdir()
        if f.suffix.lower() in image_extensions and f.name not in existing_descriptions
    ]
    
    print(f"需要处理的新图片数量: {len(image_files)}")
    if not image_files:
        print("没有新的图片需要处理")
        return descriptions

    # 提高并发数到100，该任务实测峰值跑到800请求/分钟
    semaphore = asyncio.Semaphore(100)
    
    # 添加请求统计
    start_time = time.time()
    processed_count = 0

    async def process_single_image(image_path):
        nonlocal processed_count
        async with semaphore:
            description = await get_image_description(str(image_path))
            processed_count += 1
            
            # 每处理20个请求输出一次统计
            if processed_count % 20 == 0:
                elapsed_time = time.time() - start_time
                rate = processed_count / (elapsed_time / 60)
                print(f"当前处理速率: {rate:.2f} 请求/分钟")
            
            return image_path.name, description
    
    # 创建所有任务
    tasks = [process_single_image(image_path) for image_path in image_files]
    
    # 执行所有任务并等待结果
    results = await asyncio.gather(*tasks)
    
    # 将结果存入字典
    descriptions.update(dict(results))
    
    # 保存结果到JSON文件
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(descriptions, f, ensure_ascii=False, indent=2)
    
    return descriptions

async def main():
    base_dir = Path("/root/rawdata/gcs/textbook_ocr")
    
    # 获取所有以数字开头的子文件夹
    subfolders = [
        f for f in base_dir.glob("*") 
        if f.is_dir() and f.name[0].isdigit()
    ]

    print(f"找到 {len(subfolders)} 个以数字开头的文件夹需要处理")
    
    for folder in subfolders:
        folder_name = folder.name
        image_dir = folder / "auto" / "figures"
        output_dir = folder / "auto"
        
        if not image_dir.exists():
            print(f"跳过 {folder_name}: figures目录不存在")
            continue
            
        print(f"\n处理文件夹: {folder_name}")
        print(f"输入目录: {image_dir}")
        print(f"输出目录: {output_dir}")
        
        descriptions = await process_images(
            str(image_dir), 
            str(output_dir),
            folder_name
        )
        print(f"完成处理 {folder_name}: 共 {len(descriptions)} 张图片")

if __name__ == "__main__":
    asyncio.run(main())