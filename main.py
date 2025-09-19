import argparse

from loguru import logger

from app.ingest import ingest_document_to_qdrant


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse a doc and ingest paragraphs into Qdrant")
    parser.add_argument("doc_id", help="Unique document id for collection naming and payload")
    parser.add_argument("start_url", help="Start URL")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--next-selector", type=str, default=None)
    parser.add_argument("--next-text", type=str, default="Показать еще")
    parser.add_argument("--content-selector", type=str, default=".reader_article_body")
    parser.add_argument("--article-regex", type=str, default=r"^Статья\s+\d+[\.|\-]?",
                        help="Regex to detect article headings; use '' to disable")
    parser.add_argument("--no-article-grouping", action="store_true",
                        help="Do not group paragraphs into articles; single document chunk")
    parser.add_argument("--headless", action="store_true")

    # Qdrant connection
    parser.add_argument("--qdrant-url", type=str, default=None)
    parser.add_argument("--qdrant-host", type=str, default=None)
    parser.add_argument("--qdrant-port", type=int, default=None)
    parser.add_argument("--qdrant-grpc-port", type=int, default=None)
    parser.add_argument("--qdrant-api-key", type=str, default=None)

    # Embeddings
    parser.add_argument(
        "--embedding-model",
        type=str,
        default="ai-forever/FRIDA",
        help="HF model id for embeddings; FRIDA uses CLS pooling and supports prompts",
    )

    parser.add_argument("--collection-prefix", type=str, default="docs_")
    parser.add_argument("--no-recreate", action="store_true", help="Do not delete previous chunks for the doc")

    args = parser.parse_args()

    ingest_document_to_qdrant(
        doc_id=args.doc_id,
        start_url=args.start_url,
        collection_prefix=args.collection_prefix,
        recreate=not args.no_recreate,
        next_selector=args.next_selector,
        next_text=args.next_text,
        headless=args.headless,
        max_pages=args.max_pages,
        content_selector=args.content_selector,
        article_regex=(args.article_regex if args.article_regex else None),
        disable_article_grouping=args.no_article_grouping,
        qdrant_url=args.qdrant_url,
        qdrant_host=args.qdrant_host,
        qdrant_port=args.qdrant_port,
        # qdrant_grpc_port=args.qdrant_grpc_port,
        qdrant_api_key=args.qdrant_api_key,
        embedding_model=args.embedding_model,
    )


if __name__ == "__main__":
    main()


