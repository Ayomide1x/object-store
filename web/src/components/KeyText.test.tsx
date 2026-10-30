import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { KeyText } from "./KeyText";

describe("KeyText", () => {
  // Mutation u9: KeyText renders the key with dangerouslySetInnerHTML.
  it("renders markup in a key as text", () => {
    const hostile = `<img src=x onerror="window.pwned=1">`;
    const { container } = render(<KeyText value={hostile} />);
    expect(screen.getByText(hostile)).toBeInTheDocument();
    expect(container.querySelector("img")).toBeNull();
  });
});
