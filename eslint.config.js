import tseslint from "typescript-eslint";

export default tseslint.config(
	{
		ignores: ["node_modules/**", ".wrangler/**", "worker-configuration.d.ts"],
	},
	...tseslint.configs.recommended,
	{
		rules: {
			"@typescript-eslint/no-unused-vars": [
				"error",
				{ argsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" },
			],
		},
	},
	{
		files: ["reporter/**/*.js"],
		rules: {
			"@typescript-eslint/no-require-imports": "off",
		},
	},
);
