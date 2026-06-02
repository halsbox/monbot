# Mattermost setup for Monbot

This repo now has a Mattermost-only entrypoint that is DM-only and uses a single slash command:

- `/monbot`

All previous bot actions are exposed as subcommands under that one trigger, so Mattermost does not need a global command namespace for `help`, `graphs`, `report`, and so on.

## What each Mattermost env var means

Set these in `.env` or in the deployment environment for the `monbot-mm` service.

- `MM_URL`
  - The Mattermost server URL.
  - Example: `https://mm.csuas.ru`
  - This is the API base for Mattermost itself, not the bot service.

- `MM_TEAM`
  - The Mattermost team slug or team ID.
  - In most cases use the team name from the URL, not the display name.
  - Example: `csuas`

- `MM_BOT_TOKEN`
  - The bot account token created in Mattermost.
  - Used by the service to post DMs, upload graph/report files, and edit its own posts.
  - Format: the long token string shown once at bot creation time.

- `MM_PUBLIC_URL`
  - The public URL of the Monbot Mattermost integration service.
  - This must be reachable by Mattermost and by the browser that opens graph/report assets.
  - This is not the Mattermost server URL.
  - Example if exposed on a separate host: `https://monbot.csuas.ru`
  - Example if reverse-proxied under the Mattermost domain: `https://mm.csuas.ru/monbot-bot`
  - The command/action/dialog endpoints are built from this value.

- `MM_WEBHOOK_SECRET`
  - Optional, but recommended.
  - When set, it becomes part of the callback paths so the endpoints are harder to guess.
  - The endpoints become:
    - `/mm/<secret>/command`
    - `/mm/<secret>/action`
    - `/mm/<secret>/dialog`
  - Generate a value such as `openssl rand -hex 16`.

- `MM_SETUP_TOKEN`
  - One-time Mattermost personal access token or admin token used only for setup.
  - Must be able to create/update slash commands for the target team.
  - The setup script uses it to register `/monbot`.
  - You can pass it with `--token` instead of storing it in `.env`.

- `MM_INITIAL_ADMINS`
  - Comma-separated Mattermost user IDs or usernames that should be seeded as admins.
  - Example: `u7m4k3g9q3p1t8v4j5x6z,alice,bob`
  - If you already know the Mattermost user ID, use that.
  - User IDs are the safer choice.

- `MM_BIND_HOST`
  - Host the integration server binds to inside the container.
  - Usually `0.0.0.0`.

- `MM_BIND_PORT`
  - Port the integration server listens on inside the container.
  - The compose file uses `8088`.

- `MM_CACHE_DIR`
  - Local path where cached graph PNG/PDF assets are stored.
  - In compose, this is mounted as `/mm-cache`.

- `MM_DB_PATH`
  - Local SQLite path used by the Mattermost side of the bot.
  - In compose, this is mounted under `/data`.

## What you need to set in practice

Minimum runtime values:

- `MM_URL`
- `MM_TEAM`
- `MM_BOT_TOKEN`
- `MM_PUBLIC_URL`

Recommended runtime values:

- `MM_WEBHOOK_SECRET`
- `MM_INITIAL_ADMINS`

One-time setup value:

- `MM_SETUP_TOKEN`

## How to get the values

- `MM_URL`
  - Copy the server URL from your Mattermost deployment.
  - If you open Mattermost at `https://mm.csuas.ru`, use that exact base URL.

- `MM_TEAM`
  - Open the team in Mattermost and look at the team URL.
  - Use the team slug, not the human-readable display name.

- `MM_BOT_TOKEN`
  - Open the bot account in Mattermost.
  - Copy the token shown during bot creation.
  - If you lost it, create a new token from the bot account.

- `MM_PUBLIC_URL`
  - Decide how the bot service will be reached from the network.
  - If the service is published on its own host, use that host.
  - If you put it behind the Mattermost reverse proxy, use the full external base path that reaches the bot service.
  - The important part is that Mattermost can call it and the browser can load `/mm/assets/...` from it.

- `MM_WEBHOOK_SECRET`
  - Run `openssl rand -hex 16`.
  - Put the output string into `.env`.

- `MM_SETUP_TOKEN`
  - Create a Mattermost personal access token for your admin user.
  - The token must have enough permission to manage slash commands for the selected team.
  - Use it only for the setup script.

- `MM_INITIAL_ADMINS`
  - Copy the Mattermost user IDs of the initial admins.
  - Separate multiple values with commas.

## Setup flow

1. Put the runtime values into `.env`.
2. Make sure the `monbot-mm` service can be reached at `MM_PUBLIC_URL`.
3. Run the setup script:

   ```bash
   monbot-mm-setup --token "$MM_SETUP_TOKEN"
   ```

   If you prefer not to install the console script, use:

   ```bash
   python -m monbot.mattermost_setup --token "$MM_SETUP_TOKEN"
   ```

4. The script will:
   - resolve the Mattermost team
   - verify the bot token
   - create or update the single `/monbot` slash command
   - remove old Monbot slash commands that still point at the same integration URL
   - print the exact command/action/dialog/asset URLs

5. Restart or redeploy `monbot-mm` if you changed environment values.
6. Test in a direct message with:

   ```text
   /monbot help
   ```

## Command surface

Available subcommands:

- `help`
- `graphs`
- `maint`
- `settz`
- `refresh`
- `report`
- `audit`
- `invite`
- `start`
- `adduser`
- `setrole`
- `listusers`
- `deluser`

Notes:

- `invite` is the user-facing alias for invitation generation.
- `start` is the invite redemption path.
- All interactions remain DM-only.
