# Template Merging Process

Follow these steps to merge updates from the template remote into your repository:

1. **Update your local main branch**
   ```sh
   git checkout main
   git pull origin main
   ```

2. **Fetch all remotes**
   ```sh
   git fetch --all
   ```

3. **Create a feature branch for the template update**
   ```sh
   git checkout -b feature/template-update
   ```

4. **Merge changes from the template remote's main branch**
   ```sh
   git merge template/main -m "Merge template updates"
   # Resolve any conflicts as needed
   ```

5. **Test and validate the merge**
   - Execute all checks defined in `./.github/workflows/__call__checks.yaml` to ensure nothing is broken.

6. **Add files to git if there are changes**
   - Use git add command to add the changes files.

7. **Push the feature branch and open a pull request**
   ```sh
   git push origin feature/template-update
   ```

8. **Switch back to main and clean up the feature branch locally**
   ```sh
   git checkout main
   git branch -d feature/template-update
   ```
