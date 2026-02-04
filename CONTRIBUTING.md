# Contributing

Thank you for your interest in contributing to the DevOps Template! This is a project skeleton repository focusing on DevOps template elements implemented in GitHub Workflows.

## How to Contribute

To contribute improvements to this template, follow the traditional fork-based workflow:

1. **Fork the repository on GitHub**
   - Navigate to the repository on GitHub
   - Click the "Fork" button in the top-right corner
   - This creates a copy of the repository under your GitHub account

2. **Clone your fork locally**
   ```bash
   git clone https://github.com/YOUR_USERNAME/devops-template.git
   cd devops-template
   ```

3. **Add the original repository as upstream**
   ```bash
   git remote add upstream https://github.com/starforge-universe/devops-template.git
   ```

4. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

5. **Make your changes**
   - Follow the existing code style and patterns
   - Ensure your changes align with the template's purpose
   - Update documentation if needed

6. **Commit and push to your fork**
   ```bash
   git add .
   git commit -m "Description of your changes"
   git push origin feature/your-feature-name
   ```

7. **Open a pull request on GitHub**
   - Navigate to your fork on GitHub
   - Click "New Pull Request"
   - Select your feature branch to merge into the original repository's main branch
   - Provide a clear description of your changes
   - Reference any related issues

8. **Ensure pull request checks pass**
   - The repository includes automated pull request checks (see `.github/workflows/pull-request-check.yaml`)
   - **All checks must pass without errors** before the pull request can be merged
   - You can run checks locally using `./.github/workflows/__call__checks.yaml` before pushing
   - If checks fail, address the issues and push additional commits to update the pull request

9. **Wait for code owner approval**
   - All changes require approval from code owners (defined in `.github/CODEOWNERS`)
   - Address any feedback or requested changes
   - Once approved and all checks pass, your pull request will be merged

### Keeping Your Fork Up to Date

Before starting new work, sync your fork with the upstream repository:

```bash
git checkout main
git fetch upstream
git merge upstream/main
git push origin main
```

This ensures your fork has the latest changes from the original repository.

## Pull Request Checks

This project includes automated pull request checks that run on every pull request:

- **Pull Request Checks Workflow** (`.github/workflows/pull-request-check.yaml`) - Validates pull requests to the main branch
- **Reusable Checks** (`.github/workflows/__call__checks.yaml`) - Executes test and validation workflows

**Important**: All pull request checks must complete successfully without errors before a pull request can be merged. Make sure to:
- Run checks locally before pushing
- Address any failing checks
- Ensure all automated tests pass

## Git History Preservation

**Important**: When using this repository as a base for your project, maintain the git history by **cloning** the repository rather than using GitHub's "Use this template" feature or creating a fresh repository.

Cloning preserves all commit history, enabling continuous adoption of template updates through git merge operations. This allows you to:
- Merge template improvements into your project
- Maintain a connection to the original template repository
- Track the evolution of your project alongside template updates

## Code of Conduct

- Be respectful and considerate in all interactions
- Provide constructive feedback
- Follow the project's coding standards and guidelines

## Questions?

If you have questions or need help, please:
- Open an issue for discussion
- Contact the code owners
- Review the [README.md](README.md) for more information about the project
