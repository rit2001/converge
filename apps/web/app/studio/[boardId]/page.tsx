import { idSchema } from "@converge/protocol";
import { Workspace } from "../../../src/components/workspace";

export default async function BoardStudioPage({
  params,
}: {
  params: Promise<{ boardId: string }>;
}): Promise<React.JSX.Element> {
  const { boardId } = await params;
  const parsed = idSchema.safeParse(boardId);
  return parsed.success ? (
    <Workspace requestedBoardId={parsed.data} />
  ) : (
    <main aria-label="Board unavailable">
      <h1>Board unavailable</h1>
      <p>This board link is not available.</p>
    </main>
  );
}
