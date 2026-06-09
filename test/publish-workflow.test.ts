import { readFile } from "node:fs/promises"
import { join } from "node:path"

describe("publish workflow", () => {
  it("rejects mismatched tag and package versions before publishing", async () => {
    const workflowPath = join(
      process.cwd(),
      ".github",
      "workflows",
      "publish.yml",
    )
    const packageJsonPath = join(process.cwd(), "package.json")

    const [workflow, packageJsonRaw] = await Promise.all([
      readFile(workflowPath, "utf8"),
      readFile(packageJsonPath, "utf8"),
    ])

    const packageJson = JSON.parse(packageJsonRaw) as { version?: string }

    expect(workflow).toContain('PACKAGE_VERSION="$(node -p "require(\'./package.json\').version")"')
    expect(workflow).toContain('TAG_VERSION="${GITHUB_REF_NAME#v}"')
    expect(workflow).toContain('if [ "$PACKAGE_VERSION" != "$TAG_VERSION" ]; then')
    expect(workflow).toContain("npm publish --provenance --access public")
    expect(packageJson.version).toBeDefined()
  })
})
