// Lint is part of the UI's safety rules, not just style:
//   - no way to put a key (user-controlled text) into the page as markup;
//   - downloads stay plain links: no JS ever holds object bytes;
//   - end-to-end tests never wait a fixed time.
import js from "@eslint/js";
import playwright from "eslint-plugin-playwright";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";
import globals from "globals";
import tseslint from "typescript-eslint";

const noMarkup = [
  { selector: "MemberExpression[property.name=/^(innerHTML|outerHTML)$/]", message: "Never write markup: render text with React." },
  { selector: "CallExpression[callee.property.name='insertAdjacentHTML']", message: "Never write markup: render text with React." },
  { selector: "CallExpression[callee.object.name='document'][callee.property.name=/^write(ln)?$/]", message: "Never write markup." },
  { selector: "CallExpression[callee.name='eval'], NewExpression[callee.name='Function']", message: "No dynamic code." },
];
const noBytesInJs = [
  { selector: "MemberExpression[object.name='URL'][property.name='createObjectURL']", message: "Downloads are plain <a download> links: the browser streams them to disk." },
  { selector: "NewExpression[callee.name='Blob']", message: "Downloads are plain <a download> links: never hold object bytes in JS." },
  { selector: "CallExpression[callee.property.name=/^(blob|arrayBuffer|bytes)$/]", message: "Downloads are plain <a download> links: never read object bytes in JS." },
];

export default tseslint.config(
  { ignores: ["node_modules", "test-results", "playwright-report"] },
  js.configs.recommended,
  ...tseslint.configs.strict,
  {
    files: ["src/**/*.{ts,tsx}"],
    languageOptions: { globals: globals.browser },
    plugins: { react, "react-hooks": reactHooks },
    settings: { react: { version: "19" } },
    rules: {
      ...reactHooks.configs.recommended.rules,
      "react/no-danger": "error",
      "react/no-danger-with-children": "error",
      "react/jsx-no-script-url": "error",
      "react/jsx-no-target-blank": "error",
      "no-restricted-syntax": ["error", ...noMarkup, ...noBytesInJs],
    },
  },
  {
    files: ["src/**/*.test.{ts,tsx}", "src/test-setup.ts"],
    rules: { "no-restricted-syntax": ["error", ...noMarkup] },
  },
  {
    files: ["e2e/**/*.ts", "playwright.config.ts", "vite.config.ts", "eslint.config.js"],
    languageOptions: { globals: globals.node },
  },
  {
    files: ["e2e/**/*.ts"],
    ...playwright.configs["flat/recommended"],
    rules: {
      ...playwright.configs["flat/recommended"].rules,
      "playwright/no-wait-for-timeout": "error",
      "playwright/no-networkidle": "error",
      "playwright/no-force-option": "error",
      "playwright/no-skipped-test": "error",
      "playwright/no-focused-test": "error",
      "playwright/no-conditional-in-test": "off",
      "playwright/no-conditional-expect": "off",
      "playwright/expect-expect": ["error", { assertFunctionNames: ["expect*"] }],
      "no-restricted-syntax": ["error",
        { selector: "CallExpression[callee.name='setTimeout']", message: "No fixed sleeps: wait for a state with expect(...)." },
        { selector: "CallExpression[callee.property.name='waitForTimeout']", message: "No fixed sleeps: wait for a state with expect(...)." },
      ],
    },
  },
);
