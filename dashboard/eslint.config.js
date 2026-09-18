import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import { defineConfig, globalIgnores } from 'eslint/config'

export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs.flat.recommended,
      reactRefresh.configs.vite,
    ],
    languageOptions: {
      globals: globals.browser,
    },
    rules: {
      // Each page loads its data in a mount effect, which this rule objects to on principle: it
      // flags any effect that calls a state-setting function, regardless of whether the setState
      // runs before or after an await. The concern it guards against is cascading renders, and for
      // three page-level loads on mount that is not a cost worth restructuring around -- the
      // alternative is adopting a data-fetching library for a demo dashboard.
      //
      // Turned off here rather than suppressed at each call site so the decision is in one place
      // with its reason. Revisit if this dashboard grows real interaction: the rule is sound
      // advice for effects that fire on user input rather than on mount.
      'react-hooks/set-state-in-effect': 'off',
    },
  },
])
