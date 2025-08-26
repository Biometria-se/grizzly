"""Grizzly provides a way to get tokens via Azure Active Directory (AAD), in the framework this is implemented by [RestApi][grizzly.users.restapi]
load user and [HTTP client][grizzly.tasks.clients.http] task, via the `@refresh_token` decorator.

It is possible to use it in custom code as well, by implementing a custom class that inherits `grizzly.auth.GrizzlyHttpAuthClient`.

For information about how to set context variables, see [Set context variable][grizzly.steps.setup.step_setup_set_context_variable].

Context variable values supports [Templating][framework.usage.variables.templating].

There are two ways to get an token, see below.

# Client secret

Using client secret for an app registration.

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.tenant" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.client.secret" to "<client secret>"
And set context variable "auth.client.resource" to "<resource url/guid>"
```

# Username and password

Using a username and password, with optional MFA authentication.

`auth.user.redirect_uri` needs to correspond to the endpoint that the client secret is registrered for.

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.provider" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.user.username" to "alice@example.onmicrosoft.com"
And set context variable "auth.user.password" to "HemL1gaArn3!"
And set context variable "auth.user.redirect_uri" to "/app-registrered-redirect-uri"
```

## MFA / TOTP

If the user is required to have a MFA method, support for software based TOTP tokens are supported. The user **must** first have this method configured.

### Configure TOTP

1. Login to the accounts [My signins](https://mysignins.microsoft.com/security-info)

2. Click on `Security info`

3. Click on `Add sign-in method`

4. Choose `Authenticator app`

5. Click on `I want to use another authenticator app`

6. Click on `Next`

7. Click on `Can't scan image?`

8. Copy `Secret key` and save it some where safe

9. Click on `Next`

10. Open a terminal and run the following command:

    /// tab | Bash
    ```bash
    OTP_SECRET="<secret key from step 8>" grizzly-cli auth
    ```
    ///

    /// tab | PowerShell
    ```powershell
    $Env:OTP_SECRET = "<secret key from step 8>"
    grizzly-cli auth
    ```
    ///

11. Copy the code generate from above command, go back to the browser and paste it into the text field and click `Next`

12. Finish the wizard

The user now have software based TOTP tokens as MFA method, where `grizzly` will act as the authenticator app.

### Example

In addition to the "Username and password" example, the context variable `auth.user.otp_secret` must also be set.

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.tenant" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.user.username" to "alice@example.onmicrosoft.com"
And set context variable "auth.user.password" to "HemL1gaArn3!"
And set context variable "auth.user.redirect_uri" to "/app-registrered-redirect-uri"
And set context variable "auth.user.otp_secret" to "asdfasdf"  # <-- `Secret key` from Step 8 in "Configure TOTP"
```
"""

from __future__ import annotations

from grizzly_common.azure.aad import AzureAadCredential

from . import RefreshToken


class AAD(RefreshToken):
    __TOKEN_CREDENTIAL_TYPE__ = AzureAadCredential
