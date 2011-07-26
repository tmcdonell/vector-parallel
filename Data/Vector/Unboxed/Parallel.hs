{-# LANGUAGE BangPatterns #-}
-- |
-- Module      : Data.Vector.Unboxed.Parallel
-- Copyright   : [2011] Trevor L. McDonell
-- License     : BSD3
--
-- Maintainer  : Trevor L. McDonell <tmcdonell@cse.unsw.edu.au>
-- Stability   : experimental
-- Portability : non-portable (GHC Extensions)
--

module Data.Vector.Unboxed.Parallel (

  map, fold, foldMap

) where

import Prelude                                  hiding ( map )

import GHC.Conc                                 ( numCapabilities )
import Control.DeepSeq
import Control.Parallel
import Data.Vector.Unboxed                      ( Vector, Unbox )
import Data.Vector.Internal.Gang
import qualified Data.Vector.Unboxed            as U
import qualified Data.Vector.Unboxed.Mutable    as M

instance Unbox a => NFData (Vector a)


-- | Map a function to each element of an array.
--
{-# INLINE map #-}
map :: (Unbox a, Unbox b) => (a -> b) -> Vector a -> Vector b
map f vec
  | numCapabilities == 1        = U.map f vec
  | otherwise                   = U.create $
      do mvec <- M.unsafeNew len
         gangST theGang (uncurry (fill mvec) . split)
         return mvec
    where
      len            = U.length vec
      pieces         = numCapabilities
      (step, remain) = len `quotRem` pieces

      split i
        | i < remain = let offset = i * (step + 1)    in (offset, offset + step)
        | otherwise  = let offset = i * step + remain in (offset, offset + step - 1)

      fill !mvec !start !end = go start
        where go !i | i > end   = return ()
                    | otherwise = M.unsafeWrite mvec i (f $ U.unsafeIndex vec i) >> go (i+1)


-- | Reduce an array to a single value. The combination function must be an
-- associative operation, and the stating element must be neutral with respect
-- to this operator; i.e. the pair must form a monoid.
--
-- For example, @0@ is neutral with respect to @(+)@, as @0 + a = a@.
--
-- These restrictions are required to support efficient parallel evaluation, as
-- the starting value may be used many times depending on the number of threads.
--
{-# INLINE fold #-}
fold :: Unbox a => (a -> a -> a) -> a -> Vector a -> a
fold c z vec
  | numCapabilities == 1        = U.foldl' c z vec
  | otherwise                   = reduce segs vec
    where
      len  = U.length vec
      segs = splitChunk numCapabilities len

      reduce []     _ = z
      reduce (s:ss) v =
        let (xs, ys) = U.splitAt s v
            x        = U.foldl' c z xs
            y        = reduce ss ys
        in
        x `par` y `pseq` c x y


-- | A combination of 'map' followed by 'fold', but computed more efficiently.
-- The same restrictions apply to the reduction operator and neutral element.
--
{-# INLINE foldMap #-}
foldMap :: Unbox a => (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
foldMap f c z vec
  | numCapabilities == 1        = U.foldl' (flip (c . f)) z vec
  | otherwise                   = reduce segs vec
    where
      len  = U.length vec
      segs = splitChunk numCapabilities len

      reduce []     _ = z
      reduce (s:ss) v =
        let (xs, ys) = U.splitAt s v
            x        = U.foldl' (flip (c . f)) z xs
            y        = reduce ss ys
        in
        x `par` y `pseq` c x y

--
-- foldMap :: (Unbox a, Unbox b) => (a -> b) -> (b -> b -> b) -> b -> Vector a -> b
-- foldMap f c z = fold c z . map f
-- TLM 2011-07-25: The more explicit versions below are somehow a bit slower
{--
foldMap f c z vec
  | numCapabilities == 1        = U.foldl' c z . U.map f  $ vec
  | otherwise                   = U.foldl' c z $ U.create $
      do mvec <- M.unsafeNew pieces
         gangST theGang (reduce mvec)
         return mvec
    where
      len            = U.length vec
      pieces         = numCapabilities
      (step, remain) = len `quotRem` pieces

      split i
        | i < remain = let offset = i * (step + 1)    in (offset, offset + step)
        | otherwise  = let offset = i * step + remain in (offset, offset + step - 1)

      reduce !mvec !tid = iter start z
        where
          (start, end) = split tid
          iter !i !a | i > end   = M.unsafeWrite mvec tid a
                     | otherwise = iter (i+1) (a `c` f (U.unsafeIndex vec i))
--}


-- Auxiliary
-- ---------

-- Split a range into the given number of chunks as evenly as possible. Returns
-- a list of the size of each successive chunk.
--
splitChunk :: Int -> Int -> [Int]
splitChunk pieces len =
  replicate remain (step + 1) ++ replicate (pieces - remain) step
  where
    (step, remain) = len `quotRem` pieces

{--
-- Split a range into a set of inclusive index chunks
--
splitRange :: Int -> Int -> [(Int,Int)]
splitRange pieces len = P.map split [0 .. pieces-1]
  where
    (step, remain) = len `quotRem` pieces
    split i
      | i < remain = let offset = i * (step + 1)    in (offset, offset + step)
      | otherwise  = let offset = i * step + remain in (offset, offset + step - 1)
--}

