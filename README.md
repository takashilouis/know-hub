<p align="center">
  <img alt="KNOW-hub Logo" src="backend/assets/know_hub_logo.png">
</p>
<p align="center">
  <a href='http://makeapullrequest.com'><img alt='PRs Welcome' src='https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=shields'/></a>
</p>

<!-- add a roadmap! - <a href="https://know-hub.ai/roadmap">Roadmap</a> - -->
<!-- Add a changelog! - <a href="https://know-hub.ai/changelog">Changelog</a> -->

<p align="center">
  <a href="https://know-hub.ai/docs">Docs</a> - <a href="https://discord.gg/BwMtv3Zaju">Community</a> - <a href="https://know-hub.ai/docs/blogs/gpt-vs-know-hub-multimodal">Why KNOW-hub?</a> - <a href="https://github.com/know-hub-org/know-hub-core/issues/new?assignees=&labels=bug&template=bug_report.md">Bug reports</a>
</p>

## Application Previews

![Home Thumbnail](images/home.png)
![Knowledge Base Thumbnail](images/knowledge-base.png)
![Chat with AI Thumbnail](images/chat-with-ai.png)

> Please run the migration script before launching KNOW-hub:
> ```bash
> python scripts/migrate_auth_columns_complete.py --postgres-uri "postgresql+asyncpg://user:pass@host:port/db"
> ```

## KNOW-hub is an AI-native toolset for visually rich documents and multimodal data

I am building the best way for developers to integrate context (however complex and nuanced) into their AI applications. I offer a treasure chest of tools to store, represent, and search (shallow, and deep) unstructured data. End-to-End.

## Why?

Building AI applications that interact with data shouldn't require duct-taping together a dozen different tools just to get relevant results to your LLM.

Traditional RAG approaches that work in proof-of-concepts often fail spectacularly in production. Cobbling together separate systems for text extraction, OCR, embeddings, vector databases, and retrieval creates fragile pipelines that break under real-world load. Each component brings its own APIs, configurations, and failure modes - what starts as a simple demo becomes an unmaintainable mess at scale.

Even worse, these pipelines fundamentally fail at understanding visually rich documents. Charts become meaningless text fragments. Critical diagrams lose their spatial relationships. Tables get mangled into unreadable strings. Technical specifications with mixed text and visuals? Forget about accuracy.

The result is AI applications that confidently return wrong answers because they never truly understood the documents. They miss crucial information embedded in images, misinterpret technical diasgrams, and treat visual data as an afterthought. Watch your infrastructure costs explode as your LLM re-processes the same 500-page manual for every single query.

## What?
[KNOW-hub] provides developers the tools to ingest, search (deep and shallow), transform, and manage unstructured and multimodal documents. Some of the features include:

- [Multimodal Search]: Employ techniques such as ColPali to build search that actually *understands* the visual content of documents you provide. Search over images, PDFs, videos, and more with a single endpoint.
- [Fast and Scalable Metadata Extraction]: Extract metadata from documents - including bounding boxes, labeling, classification, and more.
- [Integrations] Integrate with existing tools and workflows. Including (but not limited to) Google Suite, Slack, and Confluence.

## Table of Contents
- [Getting Started with KNOW-hub](#getting-started-with-know-hub-recommended)
- [Self-hosting KNOW-hub](#self-hosting-the-open-source-version)
- [Using KNOW-hub](#using-know-hub)
- [Contributing](#contributing)

## Getting Started with KNOW-hub (Recommended)

The fastest and easiest way to get started with KNOW-hub is by signing up for free at [KNOW-hub]. I have a generous free tier and transparent, compute-usage based pricing if you're looking to.

## Self-hosting KNOW-hub
If you'd like to self-host KNOW-hub, you can find the dedicated instruction. I offer options for direct installation and installation via docker.

For local development setup, see [running_locally.md](backend/running_locally.md).

## Using KNOW-hub

### Code (Example: Python SDK)
Ingesting a file is as simple as:

```python
from know_hub import KNOWHub

know_hub = KNOWHub("<your-know-hub-uri>")
know_hub.ingest_file("path/to/your/super/complex/file.pdf")
```

Similarly, searching and querying your data is easy too:

```python
know_hub.query("What's the height of screw 14-A in the chair assembly instructions?")
```

### KNOW-hub Console

You can also interact with KNOW-hub via the KNOW-hub Console. This is a web-based interface that allows you to ingest, search, and query your data. You can upload files, connect to different data sources, and chat with your data all within the same place.

### Model Context Protocol

Finally, you can also access KNOW-hub via MCP. Instructions are available.


## Contributing
You're welcome to contribute to the project! I love:
- Bug reports via [GitHub issues](https://github.com/takashilouis/know-hub/issues)
- Feature requests via [GitHub issues](https://github.com/takashilouis/know-hub/issues)
- Pull requests

Currently, I'm focused on improving speed, integrating with more tools, and finding the research papers that provide the most value to my users. If you have thoughts, let me know in this GitHub repo!

## License

KNOW-hub Core is **source-available** under the [Business Source License 1.1]

- **Personal / Indie use**: free.
- **Commercial production use**: free if your KNOW-hub deployment generates < $2 000/month in gross revenue.
  Otherwise purchase.
- **Future open source**: each code version automatically re-licenses to Apache 2.0 exactly four years after its first release.

See the full licence text for details.


## My website
Visit my site at [LouisKhanh.com](https://louiskhanh.com)
