import React, { createContext, useEffect, useReducer } from 'react';

type AppContextType = {
    settings: AppContext;
};

type AppContext = {
    darkMode: boolean;
};

const localStorageKeys = {
    darkMode: 'darkMode',
};

enum AppActionType {
    setDarkMode = 'setDarkMode',
    setSystemDarkMode = 'setSystemDarkMode',
}

interface Action {
    type: AppActionType;
    value: boolean;
}

const DARK_SCHEME_QUERY = '(prefers-color-scheme: dark)';

// Until the user picks a theme with the toggle (stored in localStorage), follow the OS/browser theme.
const hasStoredDarkMode = () => window.localStorage.getItem(localStorageKeys.darkMode) !== null;
const systemDarkMode = () => window.matchMedia?.(DARK_SCHEME_QUERY).matches ?? false;

const initialLocalSettings: AppContext = {
    darkMode: hasStoredDarkMode()
        ? window.localStorage.getItem(localStorageKeys.darkMode) === String(true)
        : systemDarkMode(),
};

const initialAppContext: AppContextType = {
    settings: initialLocalSettings,
};

const appContextReducer = (state: AppContextType, action: Action) => {
    switch (action.type) {
        case AppActionType.setDarkMode:
            window.localStorage.setItem(localStorageKeys.darkMode, String(action.value));
            return { ...state, settings: { ...state.settings, darkMode: action.value } };
        case AppActionType.setSystemDarkMode:
            // System theme changes are not persisted, so the app keeps following the system.
            return { ...state, settings: { ...state.settings, darkMode: action.value } };
        default:
            return state;
    }
};

type ContextType = {
    state: AppContextType;
    dispatch: React.Dispatch<Action>;
};

const AppContext = createContext<ContextType>({
    state: initialAppContext,
    dispatch: () => null,
});

// @ts-expect-error - it doesn't like the "children" prop
const WithAppContext: React.FC = ({ children }) => {
    const [state, dispatch] = useReducer(appContextReducer, initialAppContext);

    useEffect(() => {
        const mql = window.matchMedia?.(DARK_SCHEME_QUERY);
        if (!mql) return;
        const onChange = (e: MediaQueryListEvent) => {
            if (!hasStoredDarkMode()) dispatch({ type: AppActionType.setSystemDarkMode, value: e.matches });
        };
        mql.addEventListener('change', onChange);
        return () => mql.removeEventListener('change', onChange);
    }, []);

    return <AppContext.Provider value={{ state, dispatch }}>{children}</AppContext.Provider>;
};

export { WithAppContext, AppContext, AppActionType };
