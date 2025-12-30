evernote-notion-compare/
│
├── README.md
├── pyproject.toml            # or requirements.txt
├── .gitignore
│
├── tools/                    # one-time / admin utilities
│   ├── __init__.py
│   └── evernote_oauth_fetch.py
│
├── app/                      # main application code
│   ├── __init__.py
│   ├── config.py             # paths, env var names
│   ├── auth.py               # token loading helpers
│   ├── evernote_client.py    # Evernote API wrapper
│   ├── notion_client.py      # Notion API wrapper
│   └── compare.py            # comparison logic (Step 4)
│
├── cli/                      # user-facing entrypoints
│   ├── __init__.py
│   └── main.py               # argparse / click CLI
│
├── scripts/                  # convenience scripts (optional)
│   └── run_compare.sh
│
└── .secrets/                 # local only (NOT committed)
    └── evernote_token.json