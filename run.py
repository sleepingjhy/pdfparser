"""
MinerU PDF 提取管道 - CLI 入口

用法:
  python run.py run [--limit N]       运行完整管道（扫描→上传→轮询→下载→转换）
  python run.py scan                  仅扫描并注册新PDF
  python run.py status                显示处理状态统计
  python run.py retry-failed          重置所有失败文件为待处理
  python run.py convert-only          仅对已下载的原始数据重新转换
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from pipeline.config import load_config
from pipeline.processor import Processor


def setup_logging(log_file: str, verbose: bool = False) -> None:
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
    ]

    # 确保日志目录存在
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handlers.append(
        logging.FileHandler(log_file, encoding="utf-8"),
    )

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


async def cmd_run(processor: Processor, args: argparse.Namespace) -> None:
    """运行完整管道"""
    await processor.initialize()
    try:
        journals = args.journals if args.journals else None
        await processor.run(limit=args.limit, journals=journals)
    finally:
        await processor.close()


async def cmd_scan(processor: Processor, _args: argparse.Namespace) -> None:
    """仅扫描并注册新PDF"""
    await processor.initialize()
    try:
        new_count = await processor.scan_and_register()
        stats = await processor.show_status()
        print(f"\n新注册: {new_count} 个PDF")
        _print_stats(stats)
    finally:
        await processor.close()


async def cmd_status(processor: Processor, _args: argparse.Namespace) -> None:
    """显示处理状态"""
    await processor.initialize()
    try:
        stats = await processor.show_status()
        _print_stats(stats)
    finally:
        await processor.close()


async def cmd_retry_failed(processor: Processor, _args: argparse.Namespace) -> None:
    """重置失败文件"""
    await processor.initialize()
    try:
        count = await processor.retry_failed()
        print(f"已重置 {count} 个失败文件为待处理")
    finally:
        await processor.close()


async def cmd_convert_only(processor: Processor, _args: argparse.Namespace) -> None:
    """仅重新转换"""
    await processor.initialize()
    try:
        await processor.convert_only()
    finally:
        await processor.close()


def _print_stats(stats: dict[str, int]) -> None:
    """格式化打印统计信息"""
    total = stats.get("total", 0)
    print("\n" + "=" * 40)
    print("  处理状态统计")
    print("=" * 40)
    print(f"  总计:       {total}")
    print(f"  待处理:     {stats.get('pending', 0)}")
    print(f"  上传中:     {stats.get('uploading', 0)}")
    print(f"  轮询中:     {stats.get('polling', 0)}")
    print(f"  已下载:     {stats.get('downloaded', 0)}")
    print(f"  转换中:     {stats.get('converting', 0)}")
    print(f"  已完成:     {stats.get('done', 0)}")
    print(f"  失败:       {stats.get('failed', 0)}")
    print("=" * 40)
    if total > 0:
        done = stats.get("done", 0)
        print(f"  完成率:     {done / total * 100:.1f}%")
        print("=" * 40)


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="MinerU PDF 提取管道",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default=None,
        help="配置文件路径 (默认: config.yaml)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="输出详细日志",
    )

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # run
    p_run = subparsers.add_parser("run", help="运行完整管道")
    p_run.add_argument(
        "--limit",
        type=int,
        default=0,
        help="最多处理的文件数 (0=不限制)",
    )
    p_run.add_argument(
        "--journals",
        nargs="+",
        type=str,
        default=None,
        help="仅处理指定期刊 (空格分隔，如: --journals 心理学报 物理学报)",
    )

    # scan
    subparsers.add_parser("scan", help="仅扫描并注册新PDF")

    # status
    subparsers.add_parser("status", help="显示处理状态统计")

    # retry-failed
    subparsers.add_parser("retry-failed", help="重置所有失败文件为待处理")

    # convert-only
    subparsers.add_parser("convert-only", help="仅对已下载数据重新转换")

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if not args.command:
        build_parser().print_help()
        sys.exit(1)

    # 加载配置
    config = load_config(args.config)
    setup_logging(config.paths.log_file, verbose=args.verbose)

    # 创建处理器
    processor = Processor(config)

    # 命令分发
    commands = {
        "run": cmd_run,
        "scan": cmd_scan,
        "status": cmd_status,
        "retry-failed": cmd_retry_failed,
        "convert-only": cmd_convert_only,
    }

    handler = commands[args.command]

    # Windows 下需要使用 WindowsSelectorEventLoopPolicy
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(handler(processor, args))


if __name__ == "__main__":
    main()
