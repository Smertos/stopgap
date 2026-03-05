/*
Stopgap starter module

This file shows the recommended TypeScript-first authoring model for Stopgap:
- export named handlers from files in stopgap/
- wrap handlers with query(...) or mutation(...)
- deploy, then call by path (api.<module>.<export>) via stopgap.call_fn(path, args)

Example DDL setup before deploy:

CREATE EXTENSION IF NOT EXISTS plts;
CREATE EXTENSION IF NOT EXISTS stopgap;
CREATE SCHEMA IF NOT EXISTS app;

-- Deploy with:
-- stopgap deploy --db "$STOPGAP_DB" --env prod --from-schema app
*/

import { mutation, query, v } from "@stopgap/runtime";

const pets = [
  { id: 1, name: "Luna", species: "cat" },
  { id: 2, name: "Milo", species: "dog" },
];

export const getPets = query(v.object({}), async () => pets);

export const getPetByName = query(
  v.object({ name: v.string() }),
  async ({ name }) => pets.find((pet) => pet.name === name) ?? null,
);

export const createPet = mutation(
  v.object({ name: v.string(), species: v.string() }),
  async ({ name, species }) => {
    const pet = { id: pets.length + 1, name, species };
    pets.push(pet);
    return pet;
  },
);
