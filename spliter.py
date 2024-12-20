"""大文件分片与合并工具

使用示例：
1. 打包目录：
    # 基本使用（直接打包）
    python spliter.py pack /path/to/directory -o output.tar.gz

    # 打包并分片
    python spliter.py pack /path/to/directory -o output.tar.gz --split -s 1000

2. 分片文件：
    # 基本使用（默认1GB分片）
    python spliter.py split large_file.tar.gz

    # 指定分片大小(MB)和输出目录
    python spliter.py split large_file.tar.gz -s 500 -o /path/to/output

3. 合并文件：
    # 基本使用
    python spliter.py merge large_file_splits

    # 指定输出文件
    python spliter.py merge large_file_splits -o /path/to/merged_file.tar.gz

参数说明：
    pack: 打包目录
    split: 分片文件
    merge: 合并文件
    -s/--chunk-size: 分片大小(MB)，默认1000MB
    -o/--output: 输出文件/目录
    --split: 打包时是否同时分片
"""

import os
from pathlib import Path
import subprocess
import argparse
from tqdm import tqdm
import json
from datetime import datetime

def get_file_size(file_path):
    """获取文件大小（GB）"""
    return os.path.getsize(file_path) / (1024 * 1024 * 1024)

def split_large_file(file_path, chunk_size_mb=1000, output_dir=None):
    """将大文件分片，每片默认1GB"""
    file_path = Path(file_path)
    if output_dir:
        output_dir = Path(output_dir) / f"{file_path.stem}_splits"
    else:
        output_dir = file_path.parent / f"{file_path.stem}_splits"
    
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # 获取文件大小和预计分片数
    total_size = get_file_size(file_path)
    expected_chunks = int(total_size * 1024 / chunk_size_mb) + 1
    
    # 记录分片信息
    split_info = {
        "original_file": str(file_path),
        "timestamp": datetime.now().isoformat(),
        "chunk_size_mb": chunk_size_mb,
        "total_size_gb": total_size,
        "output_dir": str(output_dir)
    }
    
    print(f"开始分片: {file_path}")
    print(f"预计分片数: {expected_chunks}")
    
    # 使用split命令分片
    cmd = f"split -b {chunk_size_mb}M {file_path} {output_dir}/{file_path.stem}_part_"
    process = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE)
    
    # 使用tqdm显示进度
    with tqdm(total=expected_chunks, desc="分片进度") as pbar:
        while True:
            # 统计已生成的分片数
            current_chunks = len(list(output_dir.glob(f"{file_path.stem}_part_*")))
            pbar.n = current_chunks
            pbar.refresh()
            
            if process.poll() is not None:
                break
    
    # 保存分片信息
    info_file = output_dir / "split_info.json"
    with open(info_file, "w", encoding="utf-8") as f:
        json.dump(split_info, f, ensure_ascii=False, indent=2)
    
    print(f"文件已分片保存至: {output_dir}")
    print(f"分片信息已保存至: {info_file}")
    return output_dir

def merge_file_chunks(chunks_dir, output_file=None):
    """合并文件分片"""
    chunks_dir = Path(chunks_dir)
    
    # 读取分片信息
    info_file = chunks_dir / "split_info.json"
    if info_file.exists():
        with open(info_file, "r", encoding="utf-8") as f:
            split_info = json.load(f)
            original_file = Path(split_info["original_file"])
    else:
        original_file = Path(chunks_dir.name.replace("_splits", ""))
    
    # ���果没有指定输出文件，使用原文件名
    if not output_file:
        output_file = chunks_dir.parent / original_file.name
    
    # 获取所有分片并排序
    chunks = sorted(list(chunks_dir.glob(f"{original_file.stem}_part_*")))
    
    print(f"开始合并 {len(chunks)} 个分片...")
    with tqdm(total=len(chunks), desc="合并进度") as pbar:
        for chunk in chunks:
            cmd = f"cat {chunk} >> {output_file}"
            subprocess.run(cmd, shell=True)
            pbar.update(1)
    
    print(f"文件已合并至: {output_file}")

def pack_directory(input_dir, output_file, do_split=False, chunk_size_mb=1000):
    """打包目录并选择性分片"""
    input_dir = Path(input_dir)
    output_file = Path(output_file)
    
    print(f"开始打包目录: {input_dir}")
    
    # 获取目录中的所有文件
    all_files = list(input_dir.rglob("*"))
    files_to_pack = [f for f in all_files if f.is_file()]
    
    # 创建临时目录用于记录进度
    temp_dir = output_file.parent / ".pack_temp"
    temp_dir.mkdir(exist_ok=True)
    progress_file = temp_dir / "progress.json"
    
    # 读取已有进度
    if progress_file.exists():
        with open(progress_file, "r", encoding="utf-8") as f:
            packed_files = set(json.load(f))
    else:
        packed_files = set()
    
    # 使用tar命令打包
    with tqdm(total=len(files_to_pack), desc="打包进度") as pbar:
        for file in files_to_pack:
            if str(file) not in packed_files:
                relative_path = file.relative_to(input_dir)
                cmd = f"tar -rf {output_file} -C {input_dir} {relative_path}"
                subprocess.run(cmd, shell=True)
                packed_files.add(str(file))
                
                # 保存进度
                with open(progress_file, "w", encoding="utf-8") as f:
                    json.dump(list(packed_files), f)
            pbar.update(1)
    
    # 压缩tar文件
    print("正在压缩文件...")
    cmd = f"gzip -f {output_file}"
    subprocess.run(cmd, shell=True)
    
    # 清理临时文件
    if temp_dir.exists():
        import shutil
        shutil.rmtree(temp_dir)
    
    output_gz = Path(str(output_file) + ".gz")
    print(f"打包完成: {output_gz}")
    
    # 如果需要分片
    if do_split:
        print("开始分片...")
        split_large_file(output_gz, chunk_size_mb)

def main():
    parser = argparse.ArgumentParser(description="大文件分片与合并工具")
    parser.add_argument("action", choices=["pack", "split", "merge"], 
                       help="执行的操作：pack（打包）, split（分片）或merge（合并）")
    parser.add_argument("input_path", help="输入目录（打包时）或文件路径（分片时）或分片目录路径（合并时）")
    parser.add_argument("--output", "-o", help="输出文件（打包时）或目录（分片时）或合并后的文件路径（合并时）")
    parser.add_argument("--chunk-size", "-s", type=int, default=1000, 
                       help="分片大小（MB），默认1000MB")
    parser.add_argument("--split", action="store_true", 
                       help="打包时是否同时进行分片")
    
    args = parser.parse_args()
    
    try:
        if args.action == "pack":
            if not args.output:
                print("打包时必须指定输出文件路径 (-o/--output)")
                return 1
            pack_directory(args.input_path, args.output, args.split, args.chunk_size)
        elif args.action == "split":
            split_large_file(args.input_path, args.chunk_size, args.output)
        else:
            merge_file_chunks(args.input_path, args.output)
    except Exception as e:
        print(f"错误: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
