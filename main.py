import argparse
import asyncio
from graphdata import GraphData
from graphcrawl import GraphCrawler
from detections import DetectionFactory
from datetime import datetime
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path", 
        type=str,
        default="graph_data.db",
        help="Path to database"                        
    )
    parser.add_argument(
        "--dt-path", 
        type=str,
        default="detections",
        help="Path to detections template directory or template file. Template file must end in .yml or .yaml"                        
    )
    parser.add_argument(
        "--collect",
        action="store_true",
        default=False,
        help="Peform MS Graph collection"
    )

    parser.add_argument(
        "--debug-count",
        type=int,
        default=0,
        help="Number of ServicePrincipal enteries to fetch"
    )

    parser.add_argument(
        "--output-file", 
        type=str,
        help="Log all object output to the specified file"                        
    )

    try:
        args = parser.parse_args()

        graph_data = GraphData(args.db_path)

        if args.collect:
            asyncio.run(refresh(graph_data, debug=args.debug_count))
            return

        detections = DetectionFactory(
            graph_data, 
            args.dt_path,
            args.output_file
        )

        for dectection in detections:
            dectection.run()
            dectection.print()

    except Exception as e:
        print(f"[-] Fatal Error (see errors.log): {str(e)}")
                
    




async def refresh(graph_data, refresh_days=7, debug=0):
    if Path(graph_data.db_path).exists():
        mtime = datetime.fromtimestamp(Path(graph_data.db_path).stat().st_mtime)
        age_days = (datetime.now() - mtime).days
        if age_days >= refresh_days:
            prompt = input(f"Databasae older than {refresh_days} days. Perform refresh (y/n): ").strip().lower()
            if prompt != 'y':
                return
        else:
            return   
    async with GraphCrawler(graph_data, debug=debug) as crawler:
         await crawler.fetch()


if __name__ == "__main__":
    main()
