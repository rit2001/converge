import { BoardRecoveryError, type VerifiedBoardRecoveryMaterial } from "@converge/database";

export interface BoardRecoveryRefreshRepository {
  load(boardId: string): Promise<VerifiedBoardRecoveryMaterial>;
  refresh(boardId: string): Promise<VerifiedBoardRecoveryMaterial>;
}

export class BoardRecoveryService {
  constructor(private readonly repository: BoardRecoveryRefreshRepository) {}

  async load(boardId: string): Promise<VerifiedBoardRecoveryMaterial> {
    try {
      return await this.repository.load(boardId);
    } catch (error) {
      if (
        !(error instanceof BoardRecoveryError) ||
        (error.code !== "MISSING_REQUIRED_SNAPSHOT" && error.code !== "TAIL_LIMIT_EXCEEDED")
      )
        throw error;
      return this.repository.refresh(boardId);
    }
  }
}
