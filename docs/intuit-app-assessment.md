# Intuit app assessment questionnaire

Answers to the questionnaire Intuit asks before granting production keys on
[developer.intuit.com](https://developer.intuit.com), with the code that backs
each answer. Keep this file current when the behaviour it cites changes.

Answers marked **(org)** are facts about the company running the app, not about
this code. Whoever submits the questionnaire must confirm them.

## Authorization and authentication

| # | Question | Answer | Basis |
|---|---|---|---|
| 1 | Tested connecting, disconnecting and reconnecting with a sandbox or non-production company? | **Yes** | The UI has a connect button per environment with keys (`QBO_SANDBOX_CLIENT_ID`), plus Disconnect (revokes the token at Intuit) and Reconnect. `internal/web/server.go`, `internal/db/installations_test.go` `TestInstallationLifecycle`. |
| 2 | How often does your app refresh access tokens? | **Only when access tokens expire** | `dbTokens.AccessToken` (`cmd/qbo-bq/main.go`) reuses the cached access token until less than 5 minutes remain of its 60-minute life, and refreshes early once on an HTTP 401. |
| 3 | Does your app retry failed authorization and authentication requests? | **Yes** | On a 401 the API client refreshes the access token and retries once (`qbo.Client.Get`, `TestGetRefreshesTokenOnceOn401`). An `invalid_grant` is not retried, since it is permanent. A failed run is retried by the next hourly job. |
| 4 | On an authorization error, do you ask customers to reconnect? | **Yes** | `invalid_grant` sets the company to `needs_reauth`, and the admin UI then shows it in red with a **Reconnect** button (`db.Store.RefreshAccessToken`, `templates/index.html`). |
| 5 | Did you use the Intuit discovery document for OAuth endpoints? | **Yes** | `qbo.OAuth.Endpoints` reads the authorization, token and revocation endpoints from Intuit's discovery document for each environment (`openid_configuration` / `openid_sandbox_configuration`) and caches them for 24 hours. Discovered endpoints must be https on an `intuit.com` host before the client secret is sent to them. If the document can't be read, the built-in endpoints are used, a warning is logged and the fetch is retried after 5 minutes (`TestOAuthUsesDiscoveryDocument`, `TestOAuthDiscoveryFallsBack`). |
| 6a | Handles errors from expired access tokens? | **Yes** | Proactive refresh before expiry, plus refresh-and-retry on 401. |
| 6b | Handles errors from expired refresh tokens? | **Yes** | Intuit returns `invalid_grant`, which leads to `needs_reauth` and Reconnect (as in #4). Rotated refresh tokens are saved under a row lock so a rotation is never lost, and `refresh_token_expires_at` is stored and shown. |
| 6c | Handles invalid grant errors? | **Yes** | `qbo.ErrInvalidGrant` → `db.ErrNeedsReauth`. Other companies keep syncing (`TestRefreshAccessTokenInvalidGrant`). |
| 6d | Handles CSRF errors? | **Yes** | The OAuth `state` is 32 random bytes, stored server-side with a 10-minute TTL, used once and tied to the admin who started the flow. A missing, unknown or expired state is refused. Cross-site POSTs are refused (`sameOriginPosts`, `TestCallbackRejectsOtherUser`, `TestCrossSitePostsAreRefused`). |
| 7 | Does your app rely on the OAuth Playground or other offline tools for tokens? | **No** | Companies connect through the app's own OAuth flow. `qbo-bq import-token` exists only as an optional developer convenience. |

## API usage

| # | Question | Answer |
|---|---|---|
| 1 | Which API categories does your app use? | **Accounting API** only (scope `com.intuit.quickbooks.accounting`). Not Payments or Payroll. |
| 2 | How often does your app call the APIs for each customer? | **Daily**, plus an hourly or multiple-times-a-day option if the list offers one when scrolled. The sync job runs every hour. A quiet hour costs a single CDC request per company. Every 7 days there is also a full reconciliation. |

## QuickBooks Online versions and features

| # | Question | Answer | Basis |
|---|---|---|---|
| 1 | Which customer-facing versions of QuickBooks Online is your app designed for? | **Simple Start, Essentials, Plus, Advanced** (all of them) | The app only reads data and doesn't depend on any edition's features. Each company is synced through the same Accounting API queries. |
| 2 | Can your app handle users gaining or losing access to version-specific features? | **Yes** | Each run reads whatever the company has at that moment. A feature the company doesn't have (for example classes or purchase orders) just returns no records. Every column is nullable, so missing fields load as `NULL`. Entities sync independently, so an error on one doesn't stop the others. Records from a lost feature stay in BigQuery, and the weekly reconciling full refresh marks any that QBO no longer returns as `is_deleted`. |
| 3 | Which of these features does your app use (verified and tested)? | **Multicurrency** and **Sales tax – US companies** **(org)**: select only the ones verified against a sandbox company with that feature turned on. | The app reads these values; it never calculates currency or tax. Multicurrency fields (`CurrencyRef`, `ExchangeRate`, `HomeTotalAmt`, `HomeBalance`) are mapped on transactions, accounts, customers and vendors. Sales tax is mapped through `TxnTaxDetail` and line `TaxCodeRef`, plus the `tax_code`, `tax_rate` and `tax_agency` tables. Non-US sales tax uses the same fields but hasn't been tested with a non-US company. |
| 4 | Do you use webhooks? | **No** | Changes are pulled hourly instead. |
| 5 | Do you use the CDC operation? | **Yes** | The normal hourly sync uses the `cdc` endpoint: one request per company covers all CDC-capable entities and reports deletions. If the watermark is more than 29 days old, or an entity hits CDC's 1,000-record cap, the app falls back to a full refresh (`internal/qbo/extractor.go`, `Syncer.SyncRealm`). |
| 5a | Why do you use the CDC operation? | **Querying specific entities doesn't give me the information I need**, plus **Other**: "We mirror the whole company into a data warehouse. A query can't show which records were deleted, and CDC reports deletions. CDC also returns changes for every entity in one request, so a quiet hourly run costs one API call per company instead of about 30 queries. We don't use webhooks: the app is a scheduled batch job with no public endpoint, and webhooks only notify us of changes, so we'd still have to fetch the records." | Deletions: CDC records with `status: Deleted` are flagged `is_deleted` (`internal/schema/mapper.go`). One call: entities that share a watermark are batched into a single `cdc` request. |
| 5b | How often do you poll the CDC service? | **Hourly** | Cloud Scheduler runs `qbo-bq sync` every hour (`sync_schedule` default `7 * * * *` in `terraform/variables.tf`). |

## Error handling

| # | Question | Answer | Basis |
|---|---|---|---|
| 1 | Tested handling API errors, including syntax and validation errors? | **Yes** | Non-retryable 4xx responses (for example a 400 query/validation fault) are returned unretried with the body. 429 and 5xx get exponential backoff that honours Retry-After. A failing entity or company does not block the others (`TestGetDoesNotRetryClientErrors`, `TestGetRetries429WithBackoff`, `TestGetGivesUpAndReturnsLastResponse`). |
| 2 | Does your app capture `intuit_tid` from response headers? | **Yes** | The `intuit_tid` header is stored on every API response (`qbo.Response.IntuitTID`). It appears in every API, token-endpoint and revoke error message, so it reaches the logs, and it is saved as `intuit-tid` metadata on each archived raw response in GCS (`TestGetCapturesIntuitTID`). |
| 3 | Does your app store error information in logs that can be shared for troubleshooting? | **Yes** | Structured JSON logs go to Cloud Logging and include the realm, entity, run id, HTTP status, URL and response body. Every raw API response is archived to GCS before it is parsed. Field-level failures are recorded in BigQuery `_ingest_errors`. |
| 4 | Do you provide a way for customers to contact you for support from within your app? | **No** | This is an internal tool. Its only users are our own staff, who sign in through IAP, and support happens through internal channels. It is not offered to third-party QuickBooks customers. |

## Security

| # | Question | Answer | Basis |
|---|---|---|---|
| 1 | Has your company ever had a security breach that required notification? | **No (org)** | Must be confirmed by the submitter. |
| 2 | Do you have a security team that regularly assesses vulnerabilities and risks? | **Yes (org)** | Must be confirmed by the submitter. In code: Dependabot is enabled (`.github/dependabot.yml`), the image is `FROM scratch` and each release ships an SPDX SBOM. |
| 3 | Are the client ID and secret stored securely? | **Yes** | The secret lives in Google Secret Manager and is injected as an env var at runtime. It never enters Terraform state, is never hardcoded or logged, and never reaches the browser, because all token exchanges are server-side. Refresh tokens are AES-256-GCM encrypted in PostgreSQL. |
| 4 | Does your app use multi-factor authentication? | **Yes** | The admin UI is reachable only through Google Identity-Aware Proxy (IAP) with Google Workspace accounts that have 2-step verification. **(org)**: confirm that 2SV is enforced for the members in `iap_members`. The sync job has no user login. |
| 5 | Does your app use Captcha for authentication? | **Yes** | The admin UI is behind IAP, so users sign in with Google. Google's sign-in flow puts CAPTCHA challenges in front of suspicious login attempts. The sync job has no user login. |
| 6 | Does your app use WebSocket? | **No** | |
| 7 | Once a customer's Intuit data is in your system, can anyone other than that customer use or see it? | **No** | The data goes only to the BigQuery dataset and GCS bucket of the organization that connected the company, and access is governed by that organization's IAM. Nothing is shared with third parties. |
