# Template Merging Process

Follow these steps to merge updates from the template remote into your repository:

1. **Update your local main branch**
   Switch to the main branch and pull the latest changes.

2. **Fetch all remotes**
   Fetch all remotes to get the latest changes from all remotes.

3. **Create a feature branch for the template update**
   Create a new branch for the template update.

4. **Merge changes from the template remote's main branch**
   - Merge changes from the template remote's main branch into the current branch.
   - Resolve merge conflicts according to the following aspects.
      - You must keep dependencies of origin but version numbers must be taken from the template.
      - Ask help in every other cases to resolve merge conflicts.
      - Use git add command to add the changes files.
   - Understand the content of `./.github/workflows/__call__checks.yaml` GitHub Workflow definition and execute all the checks locally.

5. **Push the feature branch and open a pull request**
   Push the changes to the remote repository and open a pull request.

6. **Switch back to main and clean up the feature branch locally**
   Switch back to the main branch and delete the feature branch.
