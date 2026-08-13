import { BoardRecoveryError, type VerifiedBoardRecoveryMaterial } from "@converge/database";

export interface BoardRecoveryRefreshRepository {
  load(boardId: string): Promise<VerifiedBoardRecoveryMaterial>;
  refresh(boardId: string): Promise<VerifiedBoardRecoveryMaterial>;
}

export interface BoardRecoveryLoadResult {
  material: VerifiedBoardRecoveryMaterial;
  outcome: "snapshot_tail" | "refreshed";
}

export class BoardRecoveryService {
  constructor(private readonly repository: BoardRecoveryRefreshRepository) {}

  async load(boardId: string): Promise<VerifiedBoardRecoveryMaterial> {
    return (await this.loadWithOutcome(boardId)).material;
  }

  async loadWithOutcome(boardId: string): Promise<BoardRecoveryLoadResult> {
    try {
      return { material: await this.repository.load(boardId), outcome: "snapshot_tail" };
    } catch (error) {
      if (
        !(error instanceof BoardRecoveryError) ||
        (error.code !== "MISSING_REQUIRED_SNAPSHOT" && error.code !== "TAIL_LIMIT_EXCEEDED")
      )
        throw error;
      return { material: await this.repository.refresh(boardId), outcome: "refreshed" };
    }
  }
}
