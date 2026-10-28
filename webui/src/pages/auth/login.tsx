import React, { useEffect, useState } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import Card from 'react-bootstrap/Card';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import { auth, AuthenticationError, ClientError, ServerError, setup, SETUP_STATE_INITIALIZED } from '../../lib/api';
import { AlertError, Loading } from '../../lib/components/controls';
import { useAPI } from '../../lib/hooks/api';
import { LAKEFS_POST_LOGIN_NEXT, useAuth } from '../../lib/auth/authContext';
import { normalizeNext, ROUTES } from '../../lib/utils';

type NavigateState = { redirected?: boolean; next?: string };

interface SetupResponse {
    state: string;
    comm_prefs_missing?: boolean;
    login_config?: LoginConfig;
}

export interface LoginConfig {
    username_ui_placeholder?: string;
    password_ui_placeholder?: string;
    login_url: string;
    login_url_method?: 'none' | 'redirect' | 'select';
    login_failed_message?: string;
    fallback_login_url?: string;
    fallback_login_label?: string;
    login_cookie_names: string[];
    logout_url: string;
}

export const withNext = (url: string, next: string) => {
    const u = new URL(url, window.location.origin);
    u.searchParams.set('next', normalizeNext(next));
    return u.toString();
};

export const getLoginIntent = (location: ReturnType<typeof useLocation>) => {
    const st = location.state ?? {};
    const qp = new URLSearchParams(location.search);

    const redirectedFromQuery = qp.get('redirected') === 'true';
    const redirected = Boolean(st.redirected) || redirectedFromQuery;
    const next = normalizeNext(st.next ?? qp.get('next'));

    // If `redirected=true` appears in the URL, we drop it from the query and carry
    // `redirected: true` in history state for exactly one render. On the next render
    // the URL is clean, while `redirected` from state triggers the login strategy.
    // Any subsequent navigation replaces the history entry, so the state does not persist.
    qp.delete('redirected');

    const qs = qp.toString();
    const cleanUrl = `${location.pathname}${qs ? `?${qs}` : ''}${location.hash ?? ''}`;

    return { redirected, redirectedFromQuery, next, cleanUrl };
};

const AccessKeyForm = ({ loginConfig, next }: { loginConfig: LoginConfig; next: string }) => {
    const { refreshUser } = useAuth();
    const [loginError, setLoginError] = useState<React.ReactNode>(null);

    const usernamePlaceholder = loginConfig.username_ui_placeholder || 'Access Key ID';
    const passwordPlaceholder = loginConfig.password_ui_placeholder || 'Secret Access Key';

    return (
        <Form
            onSubmit={async (e) => {
                e.preventDefault();
                const form = e.target as HTMLFormElement;
                const formData = new FormData(form);
                try {
                    setLoginError(null);
                    const username = formData.get('username');
                    const password = formData.get('password');
                    await auth.login(username, password);
                    window.sessionStorage.setItem(LAKEFS_POST_LOGIN_NEXT, next);
                    await refreshUser({ useCache: false });
                } catch (err) {
                    if (err instanceof AuthenticationError) {
                        // Invalid credentials (401)
                        const message = loginConfig.login_failed_message || "The credentials don't match.";
                        setLoginError(message);
                    } else if (err instanceof ServerError) {
                        // Server errors (5xx)
                        setLoginError('A server error occurred. Please try again in a few moments.');
                    } else if (err instanceof ClientError) {
                        // Other client errors (4xx) - bad request, rate limiting, etc.
                        setLoginError('Unable to process login request. Please try again.');
                    } else {
                        // Network errors, refreshUser errors, or other unexpected errors
                        const message =
                            err instanceof Error ? err.message : 'Unable to complete login. Please try again.';
                        setLoginError(message);
                    }
                }
            }}
        >
            <Form.Group controlId="username" className="mb-3">
                <Form.Control
                    name="username"
                    type="text"
                    placeholder={usernamePlaceholder}
                    autoFocus
                    className="bg-light"
                />
            </Form.Group>

            <Form.Group controlId="password" className="mb-3">
                <Form.Control name="password" type="password" placeholder={passwordPlaceholder} className="bg-light" />
            </Form.Group>

            {!!loginError && <AlertError error={loginError} />}

            <Button variant="primary" type="submit" className="w-100 mt-3 py-2">
                Login
            </Button>
        </Form>
    );
};

const goToFallbackLogin = (loginConfig: LoginConfig, next: string) => {
    window.sessionStorage.setItem(LAKEFS_POST_LOGIN_NEXT, next);
    loginConfig.login_cookie_names?.forEach((cookie) => {
        document.cookie = `${cookie}=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT;`;
    });
    if (loginConfig.fallback_login_url) {
        window.location.href = withNext(loginConfig.fallback_login_url, next);
    }
};

const LoginForm = ({ loginConfig }: { loginConfig: LoginConfig }) => {
    const location = useLocation();

    // Resolve "next" for post-login navigation
    const state = (location.state as NavigateState | null) ?? null;
    const qp = new URLSearchParams(location.search);
    const next = normalizeNext(state?.next ?? qp.get('next'));

    return (
        <div className="d-flex align-items-center justify-content-center">
            <Card className="shadow-lg border-0 login-card">
                <Card.Header className="text-center">
                    <div className="mt-3 mb-3">
                        <img src="/logo.svg" alt="lakeFS" className="login-logo" />
                    </div>
                </Card.Header>
                <Card.Body className="p-4">
                    <AccessKeyForm loginConfig={loginConfig} next={next} />
                    <div className={'mt-2 mb-1'}>
                        {loginConfig.fallback_login_url ? (
                            <Button
                                variant="link"
                                className="text-secondary mt-2"
                                onClick={() => goToFallbackLogin(loginConfig, next)}
                            >
                                {loginConfig.fallback_login_label || 'Try another way to login'}
                            </Button>
                        ) : (
                            ''
                        )}
                    </div>
                </Card.Body>
            </Card>
        </div>
    );
};

const MicrosoftLogo = () => (
    <svg width="18" height="18" viewBox="0 0 21 21" aria-hidden="true">
        <rect x="1" y="1" width="9" height="9" fill="#f25022" />
        <rect x="11" y="1" width="9" height="9" fill="#7fba00" />
        <rect x="1" y="11" width="9" height="9" fill="#00a4ef" />
        <rect x="11" y="11" width="9" height="9" fill="#ffb900" />
    </svg>
);

// SSO-first login card: a single "Continue with ..." button, with the access-key form tucked
// behind a small link for service accounts and break-glass admins.
const SSOLoginCard = ({ loginConfig, showLocal }: { loginConfig: LoginConfig; showLocal: boolean }) => {
    const location = useLocation();
    const [localOpen, setLocalOpen] = useState(showLocal);

    const state = (location.state as NavigateState | null) ?? null;
    const qp = new URLSearchParams(location.search);
    const next = normalizeNext(state?.next ?? qp.get('next'));

    const label = loginConfig.fallback_login_label || 'Continue with SSO';
    const isMicrosoft = /microsoft|azure|entra/i.test(label);

    return (
        <div className="d-flex align-items-center justify-content-center">
            <Card className="sso-login-card">
                <Card.Body className="p-4">
                    <div className="d-flex align-items-center gap-3 mb-3">
                        <img src="/logo192.png" alt="" className="sso-login-icon" />
                        <span className="sso-login-title">lakeFS</span>
                    </div>
                    <p className="sso-login-subtitle mb-3">Sign in to continue.</p>
                    <Button
                        variant="outline-secondary"
                        className="sso-login-button w-100 d-flex align-items-center justify-content-center gap-2"
                        onClick={() => goToFallbackLogin(loginConfig, next)}
                    >
                        {isMicrosoft && <MicrosoftLogo />}
                        <span>{label}</span>
                    </Button>
                    {localOpen ? (
                        <div className="mt-4">
                            <AccessKeyForm loginConfig={loginConfig} next={next} />
                        </div>
                    ) : (
                        <Button variant="link" className="sso-login-local p-0 mt-3" onClick={() => setLocalOpen(true)}>
                            Sign in with access key
                        </Button>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
};

// SSO is the primary login when the UI is told to redirect (or offer a choice) to fallback_login_url.
const isSSOPrimary = (loginConfig: LoginConfig | undefined) =>
    !!loginConfig?.fallback_login_url &&
    (loginConfig.login_url_method === 'redirect' || loginConfig.login_url_method === 'select');

const SSO_AUTO_REDIRECT_KEY = 'lakefs_sso_auto_redirect_at';
const SSO_AUTO_REDIRECT_COOLDOWN_MS = 30000;

// Go straight to the SSO login (login_url_method=redirect) unless the user asked for the local form
// (?local=true) or we already did so a moment ago (a failed SSO round-trip must not loop).
const shouldAutoRedirect = (loginConfig: LoginConfig | undefined, search: string) => {
    if (loginConfig?.login_url_method !== 'redirect' || !loginConfig.fallback_login_url) return false;
    if (new URLSearchParams(search).get('local') === 'true') return false;
    const last = Number(window.sessionStorage.getItem(SSO_AUTO_REDIRECT_KEY) || 0);
    return Date.now() - last > SSO_AUTO_REDIRECT_COOLDOWN_MS;
};

const LoginPage = () => {
    const location = useLocation();
    const { response, error, loading } = useAPI(() => setup.getState());
    const setupResponse = response as SetupResponse | null;
    const { redirectedFromQuery, next, cleanUrl } = getLoginIntent(location);
    // Latch the decision: once the redirect starts, the cooldown timestamp makes shouldAutoRedirect()
    // false, and any re-render before the browser leaves the page would otherwise flash the login form.
    const [redirecting, setRedirecting] = useState(false);
    const autoRedirect = redirecting || shouldAutoRedirect(setupResponse?.login_config, location.search);

    useEffect(() => {
        const cfg = setupResponse?.login_config;
        if (redirecting || !autoRedirect || !cfg?.fallback_login_url) return;
        setRedirecting(true);
        window.sessionStorage.setItem(SSO_AUTO_REDIRECT_KEY, String(Date.now()));
        window.sessionStorage.setItem(LAKEFS_POST_LOGIN_NEXT, next);
        window.location.href = withNext(cfg.fallback_login_url, next);
    }, [redirecting, autoRedirect, setupResponse, next]);

    // Coming back from the IdP via the back button restores this page from bfcache mid-redirect;
    // drop the latch so the (cooled-down) SSO card shows instead of a spinner.
    useEffect(() => {
        const onPageShow = (e: PageTransitionEvent) => {
            if (e.persisted) setRedirecting(false);
        };
        window.addEventListener('pageshow', onPageShow);
        return () => window.removeEventListener('pageshow', onPageShow);
    }, []);

    // Persist next for post-login redirect
    if (next && next.startsWith('/')) window.sessionStorage.setItem(LAKEFS_POST_LOGIN_NEXT, next);

    if (loading) return <Loading />;
    if (error)
        return <AlertError error={error} className="mt-1 w-50 m-auto" onDismiss={() => window.location.reload()} />;

    // if we are not initialized, or we are not done with comm prefs, redirect to 'setup' page
    if (setupResponse && (setupResponse.state !== SETUP_STATE_INITIALIZED || setupResponse.comm_prefs_missing)) {
        return <Navigate to={{ pathname: ROUTES.SETUP, search: location.search }} replace />;
    }

    if (redirectedFromQuery) return <Navigate to={cleanUrl} replace state={{ next }} />;

    const loginConfig = setupResponse?.login_config;

    if (autoRedirect) return <Loading />;

    if (loginConfig && isSSOPrimary(loginConfig)) {
        const showLocal = new URLSearchParams(location.search).get('local') === 'true';
        return <SSOLoginCard loginConfig={loginConfig} showLocal={showLocal} />;
    }

    // Default: lakeFS login form
    return <LoginForm loginConfig={loginConfig} />;
};

export default LoginPage;
