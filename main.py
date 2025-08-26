import argparse
import asyncio
from graphdata import GraphData
from graphcrawl import GraphCrawler
from graphdiff import GraphDiff
from detections import DetectionFactory


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
        "--auth-cache",
        action="store_true",
        default=False
    )
    parser.add_argument(
        "--diff",
        action="store_true",
        default=False,
        help="Peform diff"
    )
    parser.add_argument(
        "--debug-count",
        type=int,
        default=0,
        help="Number of Service Principal entries to fetch"
    )
    parser.add_argument(
        "--output-file", 
        type=str,
        help="Log all object output to the specified file"                        
    )

    try:
        args = parser.parse_args()
       
        # XXX Testing
        if args.diff:
            graph_diff = GraphDiff()
            graph_diff.make_hash('service_principals', ["passwordCredentials", "keyCredentials"])

            graph_data = GraphData(args.db_path, graph_diff)
            asyncio.run(refresh(graph_data, args.debug_count, args.auth_cache))
           
            print(f"result: {graph_diff.results('service_principals')}")
            return
        
        
        graph_data = GraphData(args.db_path)

        if args.collect:
             asyncio.run(refresh(graph_data, args.debug_count, args.auth_cache))
             return
        elif graph_data.fresh() == False:
            prompt = input(f"Cache database missing or older than 7 days. Perform refresh (y/n): ").strip().lower()
            if prompt == 'y':
                 asyncio.run(refresh(graph_data, args.debug_count, args.auth_cache))
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
                

async def refresh(graph_data, debug=0, use_cache=False):
    async with GraphCrawler(graph_data, debug=debug, use_cache=use_cache) as crawler:
        await crawler.fetch()


if __name__ == "__main__":
    main()
